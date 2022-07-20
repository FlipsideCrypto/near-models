{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  cluster_by = ['_inserted_timestamp::DATE'],
  unique_key = 'action_id',
) }}

WITH txs AS (

  SELECT
    block_id,
    block_hash,
    tx_hash,
    block_timestamp,
    nonce,
    signature,
    tx_receiver,
    tx_signer,
    tx,
    gas_used,
    transaction_fee,
    attached_gas,
    _ingested_at,
    _inserted_timestamp
  FROM
    {{ ref('silver__transactions') }}
  WHERE
    {{ incremental_load_filter('_inserted_timestamp') }}
),
actions AS (
  SELECT
    tx_hash,
    block_id,
    block_timestamp,
    INDEX AS action_index,
    CASE
      WHEN VALUE LIKE '%CreateAccount%' THEN VALUE
      ELSE object_keys(VALUE) [0] :: STRING
    END AS action_name,
    CASE
      WHEN action_name = 'CreateAccount' THEN '{}'
      ELSE VALUE [action_name]
    END AS action_data,
    _ingested_at,
    _inserted_timestamp
  FROM
    txs,
    LATERAL FLATTEN(
      input => tx :actions
    )
),
FINAL AS (
  SELECT
    concat_ws(
      '-',
      tx_hash,
      action_index
    ) AS action_id,
    tx_hash,
    block_id,
    block_timestamp,
    action_index,
    action_name,
    TRY_PARSE_JSON(action_data) AS action_data,
    _ingested_at,
    _inserted_timestamp
  FROM
    actions
)
SELECT
  *
FROM
  FINAL
