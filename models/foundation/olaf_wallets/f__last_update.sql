{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'signer_id',
    cluster_by = ['signer_id'],
    tags = ['curated','olap']
) }}


WITH events AS (

    SELECT
        *
    FROM
        {{ ref('silver__actions_events_s3') }}
    WHERE
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
)

select
    signer_id,
    max(block_timestamp) as last_transaction
from events
group by 1 // 45.47 seconds