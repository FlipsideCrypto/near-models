{{ config(
  materialized = 'incremental',
  incremental_predicates = ["dynamic_range_predicate","block_timestamp::date"],
  incremental_strategy = 'merge',
  merge_exclude_columns = ['inserted_timestamp'],
  unique_key = 'tx_hash',
  cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,tx_signer,tx_receiver);",
  tags = ['core_v2'],
  full_refresh = false
) }}

{% if var('NEAR_MIGRATE_ARCHIVE', False) %}
    {% if execute %}
        {% do log('Migrating transactions ' ~ var('RANGE_START') ~ ' to ' ~ var('RANGE_END'), info=True) %}
        {% do log('Invocation ID: ' ~ invocation_id, info=True) %}
    {% endif %}

  SELECT
    chunk_hash,
    block_id,
    block_timestamp,
    tx_hash,
    tx_receiver,
    tx_signer,
    transaction_json,
    outcome_json,
    OBJECT_CONSTRUCT() AS status_json,
    tx_succeeded,
    gas_used,
    transaction_fee,
    attached_gas,
    _partition_by_block_number,
    transactions_final_id,
    inserted_timestamp,
    modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
  FROM
    {{ ref('_migrate_txs') }}

  {% else %}

WITH txs_with_receipts AS (
  SELECT
    chunk_hash,
    origin_block_id AS block_id,
    origin_block_timestamp AS block_timestamp,
    tx_hash,
    response_json :transaction :: variant AS transaction_json,
    response_json :transaction_outcome :outcome :: variant AS outcome_json,
    response_json :status :: variant AS status_json,
    response_json :receipts_outcome :: ARRAY AS receipts_outcome_json,
    response_json :status :Failure IS NULL AS tx_succeeded,
    partition_key AS _partition_by_block_number
  FROM
    {{ ref('silver__transactions_v2') }}

    {% if is_incremental() %}
      WHERE
        modified_timestamp >= (
          SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01')
          FROM
            {{ this }}
        )
    {% endif %}

),
determine_receipt_gas_burnt AS (
  SELECT
    tx_hash,
    SUM(
      ZEROIFNULL(VALUE :outcome :gas_burnt :: INT)
    ) AS total_gas_burnt_receipts,
    SUM(
      ZEROIFNULL(VALUE :outcome :tokens_burnt :: INT)
    ) AS total_tokens_burnt_receipts
  FROM
    txs_with_receipts,
    LATERAL FLATTEN (
      input => receipts_outcome_json
    )
  GROUP BY
    1
),
determine_attached_gas AS (
  SELECT
    tx_hash,
    SUM(
      VALUE :FunctionCall :gas :: INT
    ) AS total_attached_gas
  FROM
    txs_with_receipts,
    LATERAL FLATTEN (
      input => transaction_json :actions :: ARRAY
    )
  GROUP BY
    1
),
transactions_final AS (
  SELECT
    chunk_hash,
    block_id,
    block_timestamp,
    t.tx_hash,
    transaction_json,
    outcome_json,
    status_json,
    total_gas_burnt_receipts,
    total_tokens_burnt_receipts,
    total_attached_gas,
    tx_succeeded,
    _partition_by_block_number
  FROM
    txs_with_receipts t
    LEFT JOIN determine_receipt_gas_burnt d USING (tx_hash)
    LEFT JOIN determine_attached_gas A USING (tx_hash)
)
SELECT
  chunk_hash,
  block_id,
  block_timestamp,
  tx_hash,
  transaction_json :receiver_id :: STRING AS tx_receiver,
  transaction_json :signer_id :: STRING AS tx_signer,
  transaction_json,
  outcome_json,
  status_json,
  tx_succeeded,
  ZEROIFNULL(outcome_json :gas_burnt :: INT) + total_gas_burnt_receipts AS gas_used,
  ZEROIFNULL(outcome_json :tokens_burnt :: INT) + total_tokens_burnt_receipts AS transaction_fee,
  COALESCE(
    total_attached_gas,
    gas_used
  ) AS attached_gas,
  _partition_by_block_number,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash']
  ) }} AS transactions_final_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  transactions_final

{% endif %}
