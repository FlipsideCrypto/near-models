{{ config(
  materialized = 'incremental',
  cluster_by = ['ingested_at::DATE', 'block_timestamp::DATE'],
  unique_key = 'action_id',
  tags = ['actions']
) }}

WITH txs AS (

  SELECT
    *
  FROM
    {{ ref('transactions') }}
  WHERE
    {{ incremental_load_filter('ingested_at') }}
),
actions AS (
  SELECT
    txn_hash,
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
    ingested_at
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
      txn_hash,
      action_index
    ) AS action_id,
    txn_hash,
    block_timestamp,
    action_index,
    action_name,
    TRY_PARSE_JSON(action_data) AS action_data,
    ingested_at
  FROM
    actions
)
SELECT
  *
FROM
  FINAL
