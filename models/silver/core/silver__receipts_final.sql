{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    unique_key = 'receipt_id',
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE', '_partition_by_block_number', ],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,receipt_id,receiver_id,predecessor_id);",
    tags = ['scheduled_core'],
    full_refresh = False
) }}
-- TODO if execute block for incremental min blockdate 
WITH 
{% if var('NEAR_MIGRATE_ARCHIVE', False) %}
lake_receipts_final as (
    select * from {{ ref('silver__streamline_receipts_final') }}
    -- TODO incrementally load?
    -- TODO rename cols?
),
{% else %}

txs_with_receipts as (
select * from {{ ref('silver__transactions_v2') }}
-- TODO incremental logic
),
blocks as (
    select * from {{ ref('silver__blocks_v2') }}
    -- TODO incremental logic
),
flatten_receipts as (
    select
        tx_hash,
        -- chunk_hash,
        -- shard_id,
        value:predecessor_id::STRING as predecessor_id,
        value:priority :: int as priority,
        value:receipt :: variant as receipt_json,
        value:receipt_id :: STRING as receipt_id,
        value:receiver_id :: string as receiver_id
    from tx_response, lateral flatten(input => response_json :receipts :: ARRAY)
),
flatten_receipt_outcomes as (
    select
        value :block_hash :: STRING as block_hash,
        value:id :: STRING as receipt_id,
        value:outcome :: variant as outcome_json,
        value:proof::array as proof
    from tx_response, lateral flatten(input => response_json :receipts_outcome :: ARRAY)
),
receipts_full as (
select
    -- chunk_hash,
    -- shard_id,
    block_hash,
    tx_hash,
    r.receipt_id,
    predecessor_id,
    receiver_id,
    priority,
    receipt_json,
    outcome_json,
    proof,
    outcome_json :status :Failure IS NULL AS receipt_succeeded,
    TRY_PARSE_JSON(
        outcome_json :status :Failure
    ) AS failure_message,
    object_keys(
        failure_message
    ) [0] :: STRING AS error_type_0,
    COALESCE(
        object_keys(
            TRY_PARSE_JSON(
                failure_message [error_type_0] :kind
            )
        ) [0] :: STRING,
        failure_message [error_type_0] :kind :: STRING
    ) AS error_type_1,
    COALESCE(
        object_keys(
            TRY_PARSE_JSON(
                failure_message [error_type_0] :kind [error_type_1]
            )
        ) [0] :: STRING,
        failure_message [error_type_0] :kind [error_type_1] :: STRING
    ) AS error_type_2,
    failure_message [error_type_0] :kind [error_type_1] [error_type_2] :: STRING AS error_message
from flatten_receipts r left join flatten_receipt_outcomes ro
on r.receipt_id = ro.receipt_id
),
FINAL AS (
    SELECT
        -- chunk_hash,
        -- shard_id,
        block_hash,
        tx_hash,
        receipt_id,
        -- block_id,
        -- block_timestamp,
        predecessor_id,
        receiver_id,
        -- signer_id, -- this is more like action_signer_id tbh.. should be renamed

        receipt_json,
        outcome_json,
        -- outcome_json :receipt_ids :: ARRAY AS receipt_outcome_id,
        -- receipt_type, -- this is not a thing
        outcome_json :gas_burnt :: NUMBER AS gas_burnt,
        outcome_json :status :: variant AS status_value,
        outcome_json :logs :: ARRAY AS logs,
        proof,
        outcome_json :metadata :: variant AS metadata,
        receipt_succeeded,
        error_type_0,
        error_type_1,
        error_type_2,
        error_message
        -- _partition_by_block_number,
        -- _inserted_timestamp
    FROM
        receipts_full
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id']
    ) }} AS receipts_final_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
