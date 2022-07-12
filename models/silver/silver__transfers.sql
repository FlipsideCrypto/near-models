{{ config(
  materialized = 'incremental',
  cluster_by = ['_inserted_timestamp::DATE'],
  unique_key = 'action_id',
  incremental_strategy = 'delete+insert'
) }}

WITH action_events AS(

  SELECT
    txn_hash,
    action_id,
    action_data :deposit :: INT AS deposit
  FROM
    {{ ref('silver__actions_events') }}
  WHERE
    action_name = 'Transfer'
    AND {{ incremental_load_filter("_inserted_timestamp") }}
),
actions AS (
  SELECT
    t.txn_hash,
    A.action_id,
    t.block_timestamp,
    t.tx_receiver,
    t.tx_signer,
    A.deposit,
    t.transaction_fee,
    t.gas_used,
    t.tx_receipt [0] :id :: STRING AS receipt_object_id,
    CASE
      WHEN tx_receipt [0] :outcome :status :: STRING = '{"SuccessValue":""}' THEN TRUE
      ELSE FALSE
    END AS status,
    t._ingested_at,
    t._inserted_timestamp
  FROM
    {{ ref('silver__transactions') }} AS t
    INNER JOIN action_events AS A
    ON A.txn_hash = t.txn_hash
  WHERE
    {{ incremental_load_filter("_inserted_timestamp") }}
),
FINAL AS (
  SELECT
    txn_hash,
    action_id,
    block_timestamp,
    tx_signer,
    tx_receiver,
    deposit,
    receipt_object_id,
    transaction_fee,
    gas_used,
    status,
    _ingested_at,
    _inserted_timestamp
  FROM
    actions
)
SELECT
  *
FROM
  FINAL
