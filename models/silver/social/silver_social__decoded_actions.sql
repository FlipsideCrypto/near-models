{{ config(
    materialized = 'incremental',
    unique_key = 'action_id_social',
    cluster_by = ['_load_timestamp::date', '_partition_by_block_number'],
    tags = ['curated', 'social']
) }}

WITH all_social_receipts AS (

    SELECT
        *
    FROM
        {{ ref('silver_social__receipts') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}

        {# NOTE - 5 "maintenance" receipts prior to 75mm that create/add/revoke keys, etc. Largely irrelevant to the platform. #}
        AND _partition_by_block_number >= 75000000
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
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
        AND _partition_by_block_number >= 75000000
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
        fc._partition_by_block_number,
        fc._inserted_timestamp
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
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        join_wallet_ids
),
flattened_actions AS (
    SELECT
        concat_ws(
            '-',
            action_id,
            key
        ) AS action_id_social,
        tx_hash,
        block_id,
        block_timestamp,
        signer_id,
        key AS node,
        VALUE AS node_data,
        _load_timestamp,
        _partition_by_block_number,
        _inserted_timestamp
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
