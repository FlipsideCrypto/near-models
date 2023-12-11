{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'action_id',
    cluster_by = ['block_timestamp::date'],
    tags = ['curated', 'social']
) }}

WITH receipts AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_inserted_timestamp') }}
        {% endif %}
        AND _partition_by_block_number >= 59670000
),
from_addkey_event AS (
    SELECT
        action_id,
        tx_hash,
        block_id,
        block_timestamp,
        allowance,
        method_name,
        receiver_id,
        'AddKey' AS _source,
        _partition_by_block_number,
        _load_timestamp,
        _inserted_timestamp
    FROM
        {{ ref('silver__actions_events_addkey_s3') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_inserted_timestamp') }}
        {% endif %}
        AND receiver_id = 'social.near'
),
nested_in_functioncall AS (
    SELECT
        action_id,
        tx_hash,
        block_id,
        block_timestamp,
        args :request :actions [0] :permission :allowance :: FLOAT AS allowance,
        args :request :actions [0] :permission :method_names :: ARRAY AS method_name,
        LOWER(
            args :request :actions [0] :permission :receiver_id :: STRING
        ) AS receiver_id,
        'FunctionCall' AS _source,
        _partition_by_block_number,
        _load_timestamp,
        _inserted_timestamp
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_inserted_timestamp') }}
        {% endif %}
        AND method_name = 'add_request_and_confirm'
        AND receiver_id = 'social.near'
),
combine AS (
    SELECT
        SPLIT(
            action_id,
            '-'
        ) [0] :: STRING AS receipt_id_from_action,
        action_id,
        tx_hash,
        block_id,
        block_timestamp,
        allowance,
        _source,
        _partition_by_block_number,
        _load_timestamp,
        _inserted_timestamp
    FROM
        from_addkey_event
    UNION
    SELECT
        SPLIT(
            action_id,
            '-'
        ) [0] :: STRING AS receipt_id_from_action,
        action_id,
        tx_hash,
        block_id,
        block_timestamp,
        allowance,
        _source,
        _partition_by_block_number,
        _load_timestamp,
        _inserted_timestamp
    FROM
        nested_in_functioncall
),
FINAL AS (
    SELECT
        A.receipt_id_from_action,
        A.action_id,
        A.tx_hash,
        A.block_id,
        A.block_timestamp,
        A.allowance,
        r.signer_id,
        A._source,
        A._partition_by_block_number,
        A._load_timestamp,
        A._inserted_timestamp
    FROM
        combine A
        LEFT JOIN receipts r
        ON receipt_id_from_action = r.receipt_object_id
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['action_id']
    ) }} AS social_addkey_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
