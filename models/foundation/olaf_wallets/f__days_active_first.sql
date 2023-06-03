
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['truncated_date', 'signer_id'],
    unique_key = 'signer_id',
    tags = ['timeline', 'olap']
) }}

WITH fact_actions_events AS (

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
),

account_owner AS (
    SELECT
        *
    FROM
        {{ ref('foundation__olap_account_owner') }}
    WHERE
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),

activity_by_signer AS (
SELECT
    signer_id,
    DATE_TRUNC('day', block_timestamp) AS truncated_date,
    ROW_NUMBER() OVER (PARTITION BY signer_id ORDER BY block_timestamp) AS counter
  FROM
    fact_actions_events
),

ccumulative as (
    SELECT 
        activity.signer_id,
        activity.truncated_date,
        DATE_TRUNC('day', block_timestamp) AS account_created_date,
        max(counter) as counter
    FROM activity_by_signer as activity
    left join account_owner on account_owner.signer_id = activity.signer_id
    group by 1, 2, 3
) // Time 

select * from ccumulative
// Running for more than 4 hour