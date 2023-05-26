{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'receipt_object_id',
    cluster_by = ['receiver_account_id'],
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
    block_timestamp,
    block_id,
    receipt_object_id,
    receipt_actions:predecessor_id::string as account_creator,
    _load_timestamp
from receipts
qualify row_number() over (partition by receiver_account_id order by block_timestamp asc) = 1

// Time -> 18 minutes and 31.11