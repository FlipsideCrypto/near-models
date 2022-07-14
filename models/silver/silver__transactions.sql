{{ config(
  materialized = 'incremental',
  unique_key = 'tx_hash',
  incremental_strategy = 'delete+insert',
  cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH base_transactions AS (

  SELECT
    *
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
    GET(
      tx :actions,
      0
    ) :FunctionCall :gas :: NUMBER AS attached_gas,
    _ingested_at,
    _inserted_timestamp
  FROM
    base_transactions
),
receipts AS (
  SELECT
    tx_hash,
    SUM(
      VALUE :outcome :gas_burnt :: NUMBER
    ) AS receipt_gas_burnt,
    SUM(
      VALUE :outcome :tokens_burnt :: NUMBER
    ) AS receipt_tokens_burnt
  FROM
    transactions,
    LATERAL FLATTEN(
      input => tx :receipt
    )
  GROUP BY
    1
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
    COALESCE(
      t.attached_gas,
      gas_used
    ) AS attached_gas,
    t._ingested_at,
    t._inserted_timestamp
  FROM
    transactions AS t
    JOIN receipts AS r
    ON t.tx_hash = r.tx_hash
)
SELECT
  *
FROM
  FINAL
