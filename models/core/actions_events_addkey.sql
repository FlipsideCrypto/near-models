{{ config(
  materialized = 'incremental',
  unique_key = 'action_id',
  cluster_by = ['ingested_at::DATE', 'block_timestamp::DATE'],
  tags = ['actions_events']
) }}

WITH action_events AS (

  SELECT
    *
  FROM
    {{ ref('actions_events') }}
  WHERE
    action_name = 'AddKey'
    AND {{ incremental_load_filter('ingested_at') }}
),
addkey_events AS (
  SELECT
    action_id,
    txn_hash,
    block_timestamp,
    action_data :access_key :nonce :: NUMBER AS nonce,
    action_data :public_key :: STRING AS public_key,
    action_data :access_key :permission AS permission,
    action_data :access_key :permission :FunctionCall :allowance :: FLOAT AS allowance,
    action_data :access_key :permission :FunctionCall :method_names :: ARRAY AS method_name,
    action_data :access_key :permission :FunctionCall :receiver_id :: STRING AS receiver_id,
    ingested_at
  FROM
    action_events
)
SELECT
  *
FROM
  addkey_events
