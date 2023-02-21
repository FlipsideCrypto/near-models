{{ config(
    materialized = 'incremental',
    unique_key = 'social_action_id',
    cluster_by = ['_load_timestamp::date', '_partition_by_block_number'],
    tags = ['s3_curated']
) }}
WITH all_social_receipts AS (

    SELECT
        *
    FROM
        {{ ref('silver_social__receipts') }}
    WHERE
        {# NOTE - 5 "maintenance" receipts prior to 75mm that create/add/revoke keys, etc. Largely irrelevant to the platform. #}
        _partition_by_block_number >= 75000000 
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            AND {{ partition_load_manual('no_buffer') }}
        {% else %}
            AND {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),
decoded_function_calls AS (
    SELECT
        *,
        SPLIT(
            action_id,
            '-'
        ) [0] :: STRING AS receipt_object_id
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        _partition_by_block_number >= 75000000 
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            AND {{ partition_load_manual('no_buffer') }}
        {% else %}
            AND {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
        AND SPLIT(
            action_id,
            '-'
        ) [0] :: STRING IN (
            SELECT
                DISTINCT receipt_object_id
            FROM
                all_social_receipts
        )
),
join_wallet_ids AS (
    SELECT
        fc.action_id,
        fc.tx_hash,
        fc.block_id,
        fc.block_timestamp,
        fc.method_name,
        fc.args,
        fc.deposit,
        fc.attached_gas,
        r.receiver_id,
        r.signer_id,
        r.execution_outcome,
        fc._load_timestamp,
        fc._partition_by_block_number
    FROM
        decoded_function_calls fc
        LEFT JOIN all_social_receipts r USING (receipt_object_id)
    WHERE
        method_name = 'set'
),
action_data AS (
    SELECT
        action_id,
        tx_hash,
        block_id,
        block_timestamp,
        COALESCE(
            args :data [signer_id],
            args :data :accountId
        ) AS set_action_data,
        attached_gas,
        receiver_id,
        signer_id,
        _load_timestamp,
        _partition_by_block_number
    FROM
        join_wallet_ids
),
flattened_actions AS (
    SELECT
        concat_ws(
            '-',
            action_id,
            key
        ) AS social_action_id,
        tx_hash,
        block_id,
        block_timestamp,
        signer_id,
        key AS node,
        VALUE as node_value,
        _load_timestamp,
        _partition_by_block_number
    FROM
        action_data,
        LATERAL FLATTEN (
            input => set_action_data
        )
)
SELECT
    *
FROM
    flattened_actions
