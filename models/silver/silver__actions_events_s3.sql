{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE', '_load_timestamp::DATE'],
  unique_key = 'action_id',
  tags = ['curated', 's3_curated']
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
    _load_timestamp,
    _partition_by_block_number
  FROM
    {{ ref('silver__streamline_transactions_final') }}

    {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
    WHERE
      {{ partition_load_manual('no_buffer') }}
    {% else %}
    WHERE
      {{ incremental_load_filter('_load_timestamp') }}
    {% endif %}
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
    _load_timestamp,
    _partition_by_block_number
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
    _load_timestamp,
    _partition_by_block_number
  FROM
    actions
)
SELECT
  *
FROM
  FINAL
