{{ config(
  materialized = 'incremental',
  cluster_by = ['ingested_at::DATE', 'block_timestamp::DATE'],
  unique_key = 'action_id',
  tags = ['core', 'transfers']
) }}

WITH action_events AS(

  SELECT
    txn_hash,
    action_id,
    action_data :deposit :: INT AS deposit
  FROM
    {{ ref('actions_events') }}
  WHERE
    action_name = 'Transfer'
    AND {{ incremental_load_filter("ingested_at") }}
),
txs AS (
  SELECT
    txn_hash,
    tx :receipt AS tx_receipt,
    block_timestamp,
    tx_receiver,
    tx_signer,
    transaction_fee,
    gas_used,
    CASE
      WHEN tx_receipt [0] :outcome :status :: STRING = '{"SuccessValue":""}' THEN TRUE
      ELSE FALSE
    END AS status,
    ingested_at
  FROM
    {{ ref('transactions') }}
  WHERE
    {{ incremental_load_filter("ingested_at") }}
),
receipts AS (
  SELECT
    txn_hash,
    ARRAY_AGG(
      VALUE :id
    ) AS receipt_object_id
  FROM
    txs,
    LATERAL FLATTEN(
      input => tx_receipt
    )
  GROUP BY
    1
),
actions AS (
  SELECT
    t.txn_hash,
    r.receipt_object_id,
    t.block_timestamp,
    t.tx_receiver,
    t.tx_signer,
    t.transaction_fee,
    t.gas_used,
    t.status,
    A.action_id,
    A.deposit,
    t.ingested_at
  FROM
    txs AS t
    INNER JOIN receipts AS r
    ON r.txn_hash = t.txn_hash
    INNER JOIN action_events AS A
    ON A.txn_hash = t.txn_hash
),
FINAL AS (
  SELECT
    block_timestamp,
    txn_hash,
    action_id,
    receipt_object_id,
    tx_signer,
    tx_receiver,
    deposit,
    transaction_fee,
    gas_used,
    status,
    ingested_at
  FROM
    actions
)
SELECT
  *
FROM
  FINAL
