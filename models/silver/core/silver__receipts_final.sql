{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    unique_key = 'receipt_id',
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,receipt_id,receiver_id,predecessor_id);",
    tags = ['scheduled_core', 'core_v2'],
    full_refresh = false
) }}

{% if var('NEAR_MIGRATE_ARCHIVE', False) %}
    {% if execute %}
        {% do log('Migrating receipts ' ~ var('RANGE_START') ~ ' to ' ~ var('RANGE_END'), info=True) %}
        {% do log('Invocation ID: ' ~ invocation_id, info=True) %}
    {% endif %}

    SELECT
        chunk_hash,
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        predecessor_id,
        receiver_id,
        receipt_json,
        outcome_json,
        tx_succeeded,
        receipt_succeeded,
        _partition_by_block_number,
        receipts_final_id,
        inserted_timestamp,
        modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        {{ ref('_migrate_receipts') }}

{% else %}
    {% if execute and not var("MANUAL_FIX") %}
        {% if is_incremental() %}
            {% set max_mod_query %}
            SELECT
                COALESCE(MAX(modified_timestamp), '1970-01-01') modified_timestamp
            FROM
                {{ this }}
            {% endset %}
        
            {% set max_mod = run_query(max_mod_query) [0] [0] %}
            {% do log('max_mod: ' ~ max_mod, info=True) %}
            {% set min_block_date_query %}
            SELECT
                MIN(origin_block_timestamp :: DATE) block_timestamp
            FROM
                {{ ref('silver__transactions_v2') }}
            WHERE
                modified_timestamp >= '{{max_mod}}'
            {% endset %}

            {% set min_bd = run_query(min_block_date_query) [0] [0] %}
            {% do log('min_bd: ' ~ min_bd, info=True) %}
            {% if not min_bd or min_bd == 'None' %}
                {% set min_bd = '2099-01-01' %}
                {% do log('min_bd: ' ~ min_bd, info=True) %}
            {% endif %}

            {% do log('min_block_date: ' ~ min_bd, info=True) %}

        {% endif %}
    {% endif %}

WITH txs_with_receipts AS (
    SELECT
        chunk_hash,
        origin_block_id,
        origin_block_timestamp,
        tx_hash,
        response_json,
        response_json :transaction_outcome :outcome :receipt_ids [0] :: STRING AS initial_receipt_id,
        response_json :status :Failure IS NULL AS tx_succeeded,
        partition_key AS _partition_by_block_number,
        modified_timestamp
    FROM
        {{ ref('silver__transactions_v2') }}
    {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer', 'partition_key') }}
        {% else %}
        {% if is_incremental() %}
            WHERE origin_block_timestamp :: DATE >= '{{min_bd}}'
        {% endif %}
    {% endif %}
),
blocks AS (
    SELECT
        block_id,
        block_hash,
        block_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__blocks_v2') }}
    {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer', 'partition_key') }}
        {% else %}
        {% if is_incremental() %}
            WHERE block_timestamp :: DATE >= '{{min_bd}}' :: DATE
        {% endif %}
    {% endif %}
),
flatten_receipts AS (
    SELECT
        origin_block_timestamp,
        chunk_hash,
        tx_hash,
        tx_succeeded,
        VALUE :receipt_id :: STRING AS receipt_id,
        VALUE :: variant AS receipt_json,
        _partition_by_block_number,
        modified_timestamp
    FROM
        txs_with_receipts,
        LATERAL FLATTEN(
            input => response_json :receipts :: ARRAY
        )
),
flatten_receipt_outcomes AS (
    SELECT
        VALUE :block_hash :: STRING AS block_hash,
        tx_hash,
        VALUE :id :: STRING AS receipt_id,
        VALUE :: variant AS outcome_json
    FROM
        txs_with_receipts,
        LATERAL FLATTEN(
            input => response_json :receipts_outcome :: ARRAY
        )
),
receipts_full AS (
    SELECT
        chunk_hash,
        ro.block_hash,
        block_id,
        block_timestamp,
        r.tx_hash,
        r.receipt_id,
        receipt_json,
        outcome_json,
        tx_succeeded,
        _partition_by_block_number
    FROM
        flatten_receipts r
        LEFT JOIN flatten_receipt_outcomes ro
        ON r.receipt_id = ro.receipt_id
        LEFT JOIN blocks b
        ON ro.block_hash = b.block_hash
    {% if is_incremental() and not var("MANUAL_FIX") %}
        WHERE
            GREATEST(
                COALESCE(r.modified_timestamp, '1970-01-01'),
                COALESCE(b.modified_timestamp, '1970-01-01')   
            ) >= '{{max_mod}}'
    {% endif %}
),
initial_receipt_full AS (
    SELECT
        chunk_hash,
        origin_block_id AS block_id,
        origin_block_timestamp AS block_timestamp,
        txr.tx_hash,
        initial_receipt_id AS receipt_id,
        OBJECT_CONSTRUCT(
            'predecessor_id', response_json :transaction :signer_id :: STRING,
            'priority', response_json :transaction :priority_fee :: INTEGER,
            'receipt', OBJECT_CONSTRUCT(
                    'Action', OBJECT_CONSTRUCT(
                        'actions', response_json :transaction :actions :: ARRAY,
                        'gas_price', Null,
                        'input_data_ids', Null,
                        'is_promise_yield', Null,
                        'output_data_receivers', Null,
                        'signer_id', response_json :transaction :signer_id :: STRING,
                        'signer_public_key', response_json :transaction :public_key :: STRING
                    )
                ),
            'receipt_id', initial_receipt_id :: STRING,
            'receiver_id', response_json :transaction :receiver_id :: STRING
        ) AS receipt_json,
        outcome_json,
        tx_succeeded,
        _partition_by_block_number
    FROM
        txs_with_receipts txr
    LEFT JOIN flatten_receipt_outcomes ro
    ON txr.initial_receipt_id = ro.receipt_id
    AND txr.tx_hash = ro.tx_hash
    {% if is_incremental() and not var("MANUAL_FIX") %}
        WHERE
            modified_timestamp >= '{{max_mod}}'
    {% endif %}
),
FINAL AS (
    SELECT
        chunk_hash,
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        receipt_json :predecessor_id :: STRING AS predecessor_id,
        receipt_json :receiver_id :: STRING AS receiver_id,
        receipt_json,
        outcome_json,
        tx_succeeded,
        outcome_json :outcome :status :Failure IS NULL AS receipt_succeeded,
        _partition_by_block_number
    FROM
        receipts_full
    UNION ALL
    SELECT
        chunk_hash,
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        receipt_json :predecessor_id :: STRING AS predecessor_id,
        receipt_json :receiver_id :: STRING AS receiver_id,
        receipt_json,
        outcome_json,
        tx_succeeded,
        tx_succeeded AS receipt_succeeded,
        _partition_by_block_number
    FROM
        initial_receipt_full
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id']
    ) }} AS receipts_final_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL

qualify(row_number() over (partition by receipt_id order by block_id is not null desc, modified_timestamp desc) = 1)

{% endif %}
