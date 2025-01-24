{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['fact_intents_id'],
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,receipt_id,token_id);",
    tags = ['intents','curated','scheduled_non_core']
) }}
-- depends_on: {{ ref('silver__streamline_receipts_final') }}
-- depends_on: {{ ref('silver__logs_s3') }}

{% if execute %}
    {% if is_incremental() and not var("MANUAL_FIX") %}
    {% do log("Incremental and NOT MANUAL_FIX", info=True) %}
    {% set max_mod_query %}

    SELECT  
        MAX(modified_timestamp) modified_timestamp
    FROM
        {{ this }}

    {% endset %}

        {%set max_mod = run_query(max_mod_query) [0] [0] %}
        {% if not max_mod or max_mod == 'None' %}
            {% set max_mod = '2099-01-01' %}
        {% endif %}

        {% do log("max_mod: " ~ max_mod, info=True) %}
        {% set min_block_date_query %}

    SELECT 
        MIN(
            block_timestamp::DATE
        )
    FROM
        (
            SELECT
                MIN(block_timestamp) block_timestamp
            FROM  
                {{ ref('silver__streamline_receipts_final') }}
            WHERE
                modified_timestamp >= '{{max_mod}}'
            UNION ALL
            SELECT
                MIN(block_timestamp) block_timestamp
            FROM
                {{ ref('silver__logs_s3') }}
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

WITH intent_txs AS (

    SELECT
        DISTINCT tx_hash
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        block_timestamp >= '2024-11-01'
        AND receiver_id = 'intents.near'
        AND receipt_actions :receipt :Action :actions [0] :FunctionCall :method_name :: STRING = 'execute_intents'

    {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        {% if is_incremental() %}
            WHERE block_timestamp::DATE >= '{{min_bd}}'
        {% endif %}
    {% endif %}
),
nep245_logs AS (
    SELECT
        block_timestamp,
        block_id,
        l.tx_hash,
        receipt_object_id AS receipt_id,
        log_index,
        receiver_id,
        predecessor_id,
        signer_id,
        gas_burnt,
        TRY_PARSE_JSON(clean_log) :event :: STRING AS log_event,
        TRY_PARSE_JSON(clean_log) :data :: ARRAY AS log_data,
        ARRAY_SIZE(log_data) AS log_data_len,
        receipt_succeeded
    FROM
        {{ ref('silver__logs_s3') }}
        l
        JOIN intent_txs r
        ON r.tx_hash = l.tx_hash
    WHERE
        receiver_id = 'intents.near'
        AND block_timestamp >= '2024-11-01'
        AND TRY_PARSE_JSON(clean_log) :standard :: STRING = 'nep245'

        {% if is_incremental() and not var("MANUAL_FIX") %}
        WHERE
            GREATEST(
                COALESCE(l.modified_timestamp, '1970-01-01'),
                COALESCE(r.modified_timestamp, '1970-01-01')   
            ) >= '{{max_mod}}'
    {% endif %}
),
flatten_logs AS (
    SELECT
        l.block_timestamp,
        l.block_id,
        l.tx_hash,
        l.receipt_id,
        l.receiver_id,
        l.predecessor_id,
        l.log_event,
        l.gas_burnt,
        this AS log_event_this,
        INDEX AS log_event_index,
        VALUE :amounts :: ARRAY AS amounts,
        VALUE :token_ids :: ARRAY AS token_ids,
        VALUE :owner_id :: STRING AS owner_id,
        VALUE :old_owner_id :: STRING AS old_owner_id,
        VALUE :new_owner_id :: STRING AS new_owner_id,
        VALUE :memo :: STRING AS memo,
        l.log_index,
        l.receipt_succeeded,
        ARRAY_SIZE(amounts) AS amounts_size,
        ARRAY_SIZE(token_ids) AS token_ids_size
    FROM
        nep245_logs l,
        LATERAL FLATTEN(
            input => log_data
        )
),
flatten_arrays AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        log_index,
        log_event_index,
        owner_id,
        old_owner_id,
        new_owner_id,
        memo,
        INDEX AS amount_index,
        VALUE :: STRING AS amount_raw,
        token_ids [INDEX] AS token_id,
        gas_burnt,
        receipt_succeeded
    FROM
        flatten_logs,
        LATERAL FLATTEN(
            input => amounts
        )
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'receipt_id', 'log_index', 'log_event_index', 'amount_index']
    ) }} AS fact_intents_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_arrays
