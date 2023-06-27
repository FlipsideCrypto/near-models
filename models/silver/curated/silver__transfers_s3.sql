{{ config(
  materialized = 'incremental',
  cluster_by = ['block_timestamp::DATE', '_load_timestamp::DATE'],
  unique_key = 'action_id',
  incremental_strategy = 'delete+insert',
  tags = ['curated']
) }}

WITH action_events AS(

  SELECT
    tx_hash,
    action_id,
    action_data :deposit :: INT AS deposit,
    _load_timestamp,
    _partition_by_block_number
  FROM
    {{ ref('silver__actions_events_s3') }}
  WHERE
    action_name = 'Transfer' 
    {% if var("MANUAL_FIX") %}
      AND {{ partition_load_manual('no_buffer') }}
    {% else %}
      AND {{ incremental_load_filter("_load_timestamp") }}
    {% endif %}
),
txs AS (
  SELECT
    tx_hash,
    tx :receipt AS tx_receipt,
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
    _partition_by_block_number
  FROM
    {{ ref('silver__streamline_transactions_final') }}

    {% if var("MANUAL_FIX") %}
    WHERE
      {{ partition_load_manual('no_buffer') }}
    {% else %}
    WHERE
      {{ incremental_load_filter("_load_timestamp") }}
    {% endif %}
),
receipts AS (
  SELECT
    tx_hash,
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
    t.tx_hash,
    A.action_id,
    t.block_id,
    t.block_timestamp,
    t.tx_signer,
    t.tx_receiver,
    A.deposit,
    r.receipt_object_id,
    t.transaction_fee,
    t.gas_used,
    t.status,
    t._load_timestamp,
    t._partition_by_block_number
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
    _partition_by_block_number
  FROM
    actions
)
SELECT
  *
FROM
  FINAL
