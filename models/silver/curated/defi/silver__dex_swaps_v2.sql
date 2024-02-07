{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'dex_swaps_v2_id',
    tags = ['curated'],
) }}

WITH swap_logs AS (

    SELECT
        tx_hash,
        receipt_object_id,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        log_index,
        clean_log,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__logs_s3') }}
    WHERE
        receipt_succeeded
        AND clean_log LIKE 'Swapped%'
        AND receiver_id NOT LIKE '%dragon_bot.near' 

        {% if var("MANUAL_FIX") %}
            AND {{ partition_load_manual('no_buffer') }}
        {% else %}
            {% if var('IS_MIGRATION') %}
                AND {{ incremental_load_filter('_inserted_timestamp') }}
            {% else %}
                AND {{ incremental_load_filter('_modified_timestamp') }}
            {% endif %}
        {% endif %}
),
receipts AS (
    SELECT
        receipt_object_id,
        receipt_actions,
        receiver_id,
        signer_id,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        receipt_object_id IN (
            SELECT
                receipt_object_id
            FROM
                swap_logs
        ) 
        {% if var("MANUAL_FIX") %}
            AND {{ partition_load_manual('no_buffer') }}
        {% else %}
            {% if var('IS_MIGRATION') %}
                AND {{ incremental_load_filter('_inserted_timestamp') }}
            {% else %}
                AND {{ incremental_load_filter('_modified_timestamp') }}
            {% endif %}
        {% endif %}
),
swap_outcome AS (
    SELECT
        tx_hash,
        receipt_object_id,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        ROW_NUMBER() over (
            PARTITION BY receipt_object_id
            ORDER BY
                log_index ASC
        ) - 1 AS swap_index,
        COALESCE(SPLIT(clean_log, ',') [0], clean_log) AS LOG,
        REGEXP_REPLACE(
            LOG,
            '.*Swapped (\\d+) (.*) for (\\d+) (.*)',
            '\\1'
        ) :: INT AS amount_out_raw,
        REGEXP_REPLACE(
            LOG,
            '.*Swapped \\d+ (\\S+) for (\\d+) (.*)',
            '\\1'
        ) :: STRING AS token_out,
        REGEXP_REPLACE(
            LOG,
            '.*Swapped \\d+ \\S+ for (\\d+) (.*)',
            '\\1'
        ) :: INT AS amount_in_raw,
        REGEXP_REPLACE(
            LOG,
            '.*Swapped \\d+ \\S+ for \\d+ (.*)',
            '\\1'
        ) :: STRING AS token_in,
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
    FROM
        swap_logs
),
parse_actions AS (
    SELECT
        tx_hash,
        o.receipt_object_id,
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
        o._partition_by_block_number,
        o._inserted_timestamp,
        o._modified_timestamp
    FROM
        swap_outcome o
        LEFT JOIN receipts r USING (receipt_object_id)
),
FINAL AS (
    SELECT
        tx_hash,
        receipt_object_id,
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
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
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
