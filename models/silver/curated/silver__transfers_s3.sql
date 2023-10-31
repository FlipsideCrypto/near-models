{{ config(
  materialized = 'incremental',
  cluster_by = ['block_timestamp::DATE'],
  unique_key = 'action_id',
  incremental_strategy = 'delete+insert',
  tags = ['curated']
) }}

WITH action_events AS(

  SELECT
    block_id,
    block_timestamp,
    tx_hash,
    action_id,
    receipt_object_id,
    action_data :deposit :: INT AS deposit,
    _load_timestamp,
    _partition_by_block_number,
    _inserted_timestamp
  FROM
    {{ ref('silver__actions_events_s3') }}
  WHERE
    action_name = 'Transfer' 
  AND 
    _partition_by_block_number >= 102410000
  AND
      tx_hash in (select tx_hash from NEAR.silver.transfers_s3 where block_timestamp is null)
    
),
txs AS (
  SELECT
    tx_hash,
    tx :receipt :: ARRAY AS tx_receipt,
    block_id,
    block_timestamp,
    tx_receiver,
    tx_signer,
    transaction_fee,
    gas_used,
    CASE
      WHEN tx :receipt [0] :outcome :status :: STRING = '{"SuccessValue":""}' THEN TRUE
      ELSE FALSE
    END AS status,
    _load_timestamp,
    _partition_by_block_number,
    _inserted_timestamp
  FROM
    {{ ref('silver__streamline_transactions_final') }}

    WHERE
    _partition_by_block_number >= 102410000
  AND
      tx_hash in (select tx_hash from NEAR.silver.transfers_s3 where block_timestamp is null)
    
),
receipts AS (
  SELECT
    tx_hash,
    ARRAY_AGG(
      VALUE :id :: STRING
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
    t.tx_hash,
    A.action_id,
    A.block_id,
    A.block_timestamp,
    t.tx_signer,
    t.tx_receiver,
    A.deposit,
    r.receipt_object_id,
    t.transaction_fee,
    t.gas_used,
    t.status,
    t._load_timestamp,
    t._partition_by_block_number,
    t._inserted_timestamp
  FROM
    txs AS t
    INNER JOIN receipts AS r
    ON r.tx_hash = t.tx_hash
    INNER JOIN action_events AS A
    ON A.tx_hash = t.tx_hash
),
FINAL AS (
  SELECT
    tx_hash,
    action_id,
    block_id,
    block_timestamp,
    tx_signer,
    tx_receiver,
    deposit,
    receipt_object_id,
    transaction_fee,
    gas_used,
    status,
    _load_timestamp,
    _partition_by_block_number,
    _inserted_timestamp
  FROM
    actions
)
SELECT
  *
FROM
  FINAL
