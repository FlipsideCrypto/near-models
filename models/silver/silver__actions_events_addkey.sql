{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = 'action_id',
  cluster_by = ['_inserted_timestamp::DATE'],
) }}

WITH action_events AS (

  SELECT
    *
  FROM
    {{ ref('silver__actions_events') }}
  WHERE
    action_name = 'AddKey'
    AND {{ incremental_load_filter('_inserted_timestamp') }}
),
addkey_events AS (
  SELECT
    action_id,
    tx_hash,
    block_timestamp,
    action_data :access_key :nonce :: NUMBER AS nonce,
    action_data :public_key :: STRING AS public_key,
    action_data :access_key :permission AS permission,
    action_data :access_key :permission :FunctionCall :allowance :: FLOAT AS allowance,
    action_data :access_key :permission :FunctionCall :method_names :: ARRAY AS method_name,
    action_data :access_key :permission :FunctionCall :receiver_id :: STRING AS receiver_id,
    _ingested_at,
    _inserted_timestamp
  FROM
    action_events
)
SELECT
  *
FROM
  addkey_events
