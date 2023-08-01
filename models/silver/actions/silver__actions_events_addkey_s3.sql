{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = 'action_id',
  cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
  tags = ['actions', 'curated']
) }}

WITH action_events AS (

  SELECT
    *
  FROM
    {{ ref('silver__actions_events_s3') }}
  WHERE
    action_name = 'AddKey' 
    {% if var("MANUAL_FIX") %}
      AND {{ partition_load_manual('no_buffer') }}
    {% else %}
      AND {{ incremental_load_filter('_inserted_timestamp') }}
    {% endif %}
),
addkey_events AS (
  SELECT
    action_id,
    tx_hash,
    block_id,
    block_timestamp,
    action_data :access_key :nonce :: NUMBER AS nonce,
    action_data :public_key :: STRING AS public_key,
    action_data :access_key :permission AS permission,
    action_data :access_key :permission :FunctionCall :allowance :: FLOAT AS allowance,
    action_data :access_key :permission :FunctionCall :method_names :: ARRAY AS method_name,
    action_data :access_key :permission :FunctionCall :receiver_id :: STRING AS receiver_id,
    _partition_by_block_number,
    _load_timestamp,
    _inserted_timestamp
  FROM
    action_events
)
SELECT
  *
FROM
  addkey_events
