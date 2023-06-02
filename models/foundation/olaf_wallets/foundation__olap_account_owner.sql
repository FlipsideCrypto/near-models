{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'receiver_id',
    cluster_by = ['receiver_id'],
    tags = ['curated','olap']
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
),

old_accounts as (
    select
        receiver_id
    FROM {% if this_table_exists %}{{ this }}{% else %}(SELECT 'None' AS receiver_id) {% endif %}
)

select
    receiver_id,
    block_timestamp,
    block_id,
    receipt_object_id,
    signer_id,
    receipt_actions:predecessor_id::string as account_creator,
    _partition_by_block_number,
    _load_timestamp
from receipts
WHERE
    receiver_id NOT IN (select receiver_id from old_accounts)
qualify row_number() over (partition by receiver_id order by block_timestamp asc) = 1

-- // Time -> 18 minutes and 31.11
