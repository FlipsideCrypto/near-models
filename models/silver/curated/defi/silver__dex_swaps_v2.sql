{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'dex_swaps_v2_id',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,receipt_object_id,receiver_id,signer_id,token_out,token_in);",
    tags = ['curated','scheduled_non_core'],
) }}

-- depends on {{ ref('silver__logs_s3') }}
-- depends on {{ ref('silver__receipts_final') }}

{% if execute %}

    {% if is_incremental() and not var("MANUAL_FIX") %}
    {% do log("Incremental and not MANUAL_FIX", info=True) %}
    {% set max_mod_query %}

    SELECT
        MAX(modified_timestamp) modified_timestamp
    FROM
        {{ this }}

    {% endset %}

        {% set max_mod = run_query(max_mod_query) [0] [0] %}
        {% if not max_mod or max_mod == 'None' %}
            {% set max_mod = '2099-01-01' %}
        {% endif %}

        {% do log("max_mod: " ~ max_mod, info=True) %}

        {% set min_block_date_query %}
    SELECT
        MIN(
            block_timestamp :: DATE
        )
    FROM
        (
            SELECT
                MIN(block_timestamp) block_timestamp
            FROM
                {{ ref('silver__logs_s3') }} A
            WHERE
                modified_timestamp >= '{{max_mod}}'
            UNION ALL
            SELECT
                MIN(block_timestamp) block_timestamp
            FROM
                {{ ref('silver__receipts_final') }} A
            WHERE
                modified_timestamp >= '{{max_mod}}'
        ) 
    {% endset %}

        {% set min_bd = run_query(min_block_date_query) [0] [0] %}
        {% if not min_bd or min_bd == 'None' %}
            {% set min_bd = '2099-01-01' %}
        {% endif %}

        {% do log("min_bd: " ~ min_bd, info=True) %}

    {% endif %}

{% endif %}

WITH swap_logs AS (

    SELECT
        tx_hash,
        receipt_id,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        log_index,
        clean_log,
        _partition_by_block_number,
        modified_timestamp
    FROM
        {{ ref('silver__logs_s3') }}
    WHERE
        receipt_succeeded
        AND clean_log LIKE 'Swapped%'
        AND receiver_id NOT LIKE '%dragon_bot.near' 
        
    {% if var("MANUAL_FIX") %}
        AND {{ partition_load_manual('no_buffer') }}
    {% else %}
        {% if is_incremental() %}
            AND block_timestamp :: DATE >= '{{min_bd}}'
        {% endif %}
    {% endif %}
),
receipts AS (
    SELECT
        receipt_id, -- slated for rename to receipt_id
        receipt_json AS receipt_actions,
        receiver_id,
        receipt_json :receipt :Action :signer_id :: STRING AS signer_id,
        _partition_by_block_number,
        modified_timestamp
    FROM
        {{ ref('silver__receipts_final') }}
    WHERE
        receipt_id IN (
            SELECT
                receipt_id
            FROM
                swap_logs
        ) 

    {% if var("MANUAL_FIX") %}
        AND {{ partition_load_manual('no_buffer') }}
    {% else %}
        {% if is_incremental() %}
        AND block_timestamp :: DATE >= '{{min_bd}}'
        {% endif %}
    {% endif %}
),
swap_outcome AS (
    SELECT
        tx_hash,
        receipt_id,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        ROW_NUMBER() over (
            PARTITION BY receipt_id
            ORDER BY
                log_index ASC
        ) - 1 AS swap_index,
        COALESCE(SPLIT(clean_log, ',') [0], clean_log) AS LOG,
        REGEXP_REPLACE(
            LOG,
            '.*Swapped (\\d+) (.*) for (\\d+) (.*)',
            '\\1'
        ) :: INT AS amount_in_raw,
        REGEXP_REPLACE(
            LOG,
            '.*Swapped \\d+ (\\S+) for (\\d+) (.*)',
            '\\1'
        ) :: STRING AS token_in,
        REGEXP_REPLACE(
            LOG,
            '.*Swapped \\d+ \\S+ for (\\d+) (.*)',
            '\\1'
        ) :: INT AS amount_out_raw,
        REGEXP_REPLACE(
            LOG,
            '.*Swapped \\d+ \\S+ for \\d+ (.*)',
            '\\1'
        ) :: STRING AS token_out,
        _partition_by_block_number,
        modified_timestamp
    FROM
        swap_logs
),
parse_actions AS (
    SELECT
        tx_hash,
        o.receipt_id,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        swap_index,
        LOG,
        amount_out_raw,
        token_out,
        amount_in_raw,
        token_in,
        ARRAY_SIZE(
            receipt_actions :receipt :Action :actions
        ) AS action_ct,
        TRY_PARSE_JSON(
            TRY_BASE64_DECODE_STRING(
                CASE
                    -- Some swaps first have a register or storage action, then swap in final action (some may have both, so could be 2 or 3 actions)
                    WHEN receipt_actions :receipt :Action :actions [0] :FunctionCall :method_name IN (
                        'register_tokens',
                        'storage_deposit'
                    ) THEN receipt_actions :receipt :Action :actions [action_ct - 1] :FunctionCall :args -- <3,000 swaps across Ref and Refv2 execute multiple swaps in 1 receipt, with inputs spread across multiple action entities
                    WHEN action_ct > 1 THEN receipt_actions :receipt :Action :actions [action_ct - 1] :FunctionCall :args -- most all other swaps execute multiswaps in 1 receipt, with 1 log and 1 action (that may contain an array of inputs for multiswap, per below)
                    ELSE receipt_actions :receipt :Action :actions [0] :FunctionCall :args
                END
            )
        ) AS decoded_action,
        TRY_PARSE_JSON(
            COALESCE(
                TRY_PARSE_JSON(
                    COALESCE(
                        -- input data is stored in the decoded action
                        -- for multi-swaps, there is (often) one action with an array of input dicts that correspond with the swap index
                        decoded_action :msg,
                        -- Swap must be capitalized! Autoformat may change to "swap"
                        decoded_action :operation: Swap,
                        decoded_action
                    )
                ) :actions [swap_index],
                -- may also be stored directly in msg key, rather than within an array of size 1
                decoded_action :msg,
                -- signer test_near.near executed multistep swaps, with separate actions, and one encoded input per action
                decoded_action :actions [0]
            )
        ) AS swap_input_data,
        r.receiver_id AS receipt_receiver_id,
        r.signer_id AS receipt_signer_id,
        o._partition_by_block_number
    FROM
        swap_outcome o
        LEFT JOIN receipts r USING (receipt_id)

    {% if is_incremental() and not var("MANUAL_FIX") %}
        WHERE
            GREATEST(
                COALESCE(o.modified_timestamp, '1970-01-01'),
                COALESCE(r.modified_timestamp, '1970-01-01')   
            ) >= '{{max_mod}}'
    {% endif %}
),
FINAL AS (
    SELECT
        tx_hash,
        receipt_id AS receipt_object_id, -- slated for rename to receipt_id
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        swap_index,
        amount_out_raw,
        token_out,
        amount_in_raw,
        token_in,
        swap_input_data,
        LOG,
        _partition_by_block_number
    FROM
        parse_actions
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_object_id', 'swap_index']
    ) }} AS dex_swaps_v2_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
