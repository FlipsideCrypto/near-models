{{ config(
  materialized = 'incremental',
  unique_key = 'tx_hash',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
  tags = ['rpc']
) }}

WITH base_transactions AS (

  SELECT
    record_id,
    tx_hash,
    tx_block_index,
    offset_id,
    block_id,
    block_timestamp,
    network,
    chain_id,
    tx,
    _ingested_at,
    _inserted_timestamp
  FROM
    {{ ref('bronze__transactions') }}
  WHERE
    {{ incremental_load_filter('_inserted_timestamp') }}
    qualify ROW_NUMBER() over (
      PARTITION BY tx_hash
      ORDER BY
        _inserted_timestamp DESC
    ) = 1
),
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
    block_id AS block_id,
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
    _ingested_at,
    _inserted_timestamp
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
    t._ingested_at,
    t._inserted_timestamp,
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
  _INGESTED_AT,
  _INSERTED_TIMESTAMP,
  attached_gas,
  tx_status
FROM
  FINAL
