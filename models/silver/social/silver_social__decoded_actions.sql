{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'action_id_social',
    cluster_by = ['_inserted_timestamp::date', '_partition_by_block_number'],
    tags = ['curated', 'social']
) }}

WITH all_social_receipts AS (

    SELECT
        receipt_object_id,
        receiver_id,
        predecessor_id,
        signer_id,
        execution_outcome,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver_social__receipts') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {% if var('IS_MIGRATION') %}
                {{ incremental_load_filter('_inserted_timestamp') }}
            {% else %}
                {{ incremental_load_filter('_modified_timestamp') }}
            {% endif %}
        {% endif %}

        {# NOTE - 5 "maintenance" receipts prior to 75mm that create/add/revoke keys, etc. Largely irrelevant to the platform. #}
        AND _partition_by_block_number >= 75000000
),
decoded_function_calls AS (
    SELECT
        SPLIT(
            action_id,
            '-'
        ) [0] :: STRING AS receipt_object_id,
        action_id,
        tx_hash,
        block_id,
        block_timestamp,
        method_name,
        args,
        deposit,
        attached_gas,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {% if var('IS_MIGRATION') %}
                {{ incremental_load_filter('_inserted_timestamp') }}
            {% else %}
                {{ incremental_load_filter('_modified_timestamp') }}
            {% endif %}
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
        r.predecessor_id,
        r.signer_id,
        r.execution_outcome,
        fc._partition_by_block_number,
        fc._inserted_timestamp,
        fc._modified_timestamp
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
            args :data [predecessor_id],
            args :data [signer_id],
            args :data :accountId
        ) AS set_action_data,
        attached_gas,
        receiver_id,
        predecessor_id,
        signer_id,
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
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
        predecessor_id,
        key AS node,
        VALUE AS node_data,
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
    FROM
        action_data,
        LATERAL FLATTEN (
            input => set_action_data
        )
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['action_id_social']
    ) }} AS social_decoded_actions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flattened_actions
