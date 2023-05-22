{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'receipt_object_id',
    cluster_by = ['_load_timestamp::DATE'],
    tags = ['receipt_map','olaf']
) }}


WITH receipts AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
)


select
    distinct receiver_id as receiver_account_id,
    receipt_object_id,
    receipt_actions:predecessor_id::string as account_creator,
    _load_timestamp
from receipts
