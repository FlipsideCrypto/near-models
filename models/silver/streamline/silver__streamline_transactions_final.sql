{{ config(
  materialized = 'incremental',
  unique_key = 'tx_hash',
  incremental_strategy = 'merge',
  cluster_by = ['_load_timestamp::date', 'block_timestamp::date'],
  tags = ['s3', 's3_third']
) }}

WITH int_txs AS (

  SELECT
    *
  FROM
    {{ ref('silver__streamline_transactions') }}
{% if is_incremental() %}
WHERE
  _load_timestamp > (
    SELECT
      MAX(_load_timestamp)
    FROM
      {{ ref('silver__streamline_receipts_final') }}
  )
{% endif %}
),
int_receipts AS (
  SELECT
    *
  FROM
    {{ ref('silver__streamline_receipts_final') }}

{% if is_incremental() %}
WHERE
  _load_timestamp > (
    SELECT
      MAX(_load_timestamp)
    FROM
      {{ ref('silver__streamline_receipts_final') }}
  )
{% endif %}
),
receipt_array AS (
  SELECT
    tx_hash,
    ARRAY_AGG(execution_outcome) AS receipt
  FROM
    int_receipts
  GROUP BY
    1
),
base_transactions AS (
  SELECT
    t.tx_hash,
    block_id,
    block_hash,
    block_timestamp,
    shard_id,
    transactions_index,
    _tx_load_timestamp,
    _block_load_timestamp,
    chunk_hash,
    outcome_receipts,
    OBJECT_CONSTRUCT(
      'actions',
      _actions,
      'hash',
      _hash,
      'nonce',
      _nonce,
      'outcome',
      _outcome,
      'public_key',
      _public_key,
      'receipt',
      r.receipt,
      'receiver_id',
      _receiver_id,
      'signature',
      _signature,
      'signer_id',
      _signer_id
    ) AS tx,
    _block_load_timestamp AS _load_timestamp,
    _partition_by_block_number
  FROM
    int_txs t
    LEFT JOIN receipt_array r USING (tx_hash)
),
{# The following steps were copied directly from legacy tx model to replicate columns #}
actions AS (
  SELECT
    tx_hash,
    SUM(
      VALUE :FunctionCall :gas
    ) AS attached_gas
  FROM
    base_transactions,
    LATERAL FLATTEN(
      input => tx :actions
    )
  GROUP BY
    1
),
transactions AS (
  SELECT
    block_id,
    tx :outcome :block_hash :: STRING AS block_hash,
    tx_hash,
    block_timestamp,
    tx :nonce :: NUMBER AS nonce,
    tx :signature :: STRING AS signature,
    tx :receiver_id :: STRING AS tx_receiver,
    tx :signer_id :: STRING AS tx_signer,
    tx,
    tx :outcome :outcome :gas_burnt :: NUMBER AS transaction_gas_burnt,
    tx :outcome :outcome :tokens_burnt :: NUMBER AS transaction_tokens_burnt,
    _load_timestamp,
    _partition_by_block_number
  FROM
    base_transactions
),
receipts AS (
  SELECT
    tx_hash,
    IFF(
      VALUE :outcome :status :Failure IS NOT NULL,
      'Fail',
      'Success'
    ) AS success_or_fail,
    SUM(
      VALUE :outcome :gas_burnt :: NUMBER
    ) over (
      PARTITION BY tx_hash
      ORDER BY
        tx_hash DESC
    ) AS receipt_gas_burnt,
    SUM(
      VALUE :outcome :tokens_burnt :: NUMBER
    ) over (
      PARTITION BY tx_hash
      ORDER BY
        tx_hash DESC
    ) AS receipt_tokens_burnt
  FROM
    transactions,
    LATERAL FLATTEN(
      input => tx :receipt
    )
),
FINAL AS (
  SELECT
    t.block_id,
    t.block_hash,
    t.tx_hash,
    t.block_timestamp,
    t.nonce,
    t.signature,
    t.tx_receiver,
    t.tx_signer,
    t.tx,
    t.transaction_gas_burnt + r.receipt_gas_burnt AS gas_used,
    t.transaction_tokens_burnt + r.receipt_tokens_burnt AS transaction_fee,
    t._load_timestamp,
    t._partition_by_block_number,
    COALESCE(
      actions.attached_gas,
      gas_used
    ) AS attached_gas,
    LAST_VALUE(
      r.success_or_fail
    ) over (
      PARTITION BY r.tx_hash
      ORDER BY
        r.success_or_fail DESC
    ) AS tx_status
  FROM
    transactions AS t
    JOIN receipts AS r
    ON t.tx_hash = r.tx_hash
    JOIN actions
    ON t.tx_hash = actions.tx_hash
)
SELECT
  DISTINCT tx_hash,
  block_id,
  block_hash,
  block_timestamp,
  nonce,
  signature,
  tx_receiver,
  tx_signer,
  tx,
  gas_used,
  transaction_fee,
  _load_timestamp,
  _partition_by_block_number,
  attached_gas,
  tx_status
FROM
  FINAL
