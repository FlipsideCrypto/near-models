
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['signer_account_id'],
    unique_key = 'signer_account_id',
    tags = ['timeline', 'olaf']
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
    LIMIT 200000000
),



txns AS (
    select 
    signer_id,
    min(block_timestamp) as account_created_date
    from fact_actions_events
    group by 1
)


activity_by_time as (
    SELECT
        txns.signer_id as signer_account_id,
        min(block_timestamp) as account_created_date,
        count(distinct case when txns.block_timestamp between txns.account_created_date and txns.account_created_date + INTERVAL '6 day' then txns.block_timestamp end) as days_active_first_7d,
        count(distinct case when txns.block_timestamp between txns.account_created_date and txns.account_created_date + INTERVAL '29 day' then txns.block_timestamp end) as days_active_first_30d,
        count(distinct case when txns.block_timestamp between txns.account_created_date + INTERVAL '29 day' and txns.account_created_date + INTERVAL '59 day' then txns.block_timestamp end) as days_active_first_30_60d,
        count(distinct case when txns.block_timestamp between txns.account_created_date + INTERVAL '59 day' and txns.account_created_date + INTERVAL '89 day' then txns.block_timestamp end) as days_active_first_60_90d,
        count(distinct case when txns.block_timestamp between txns.account_created_date + INTERVAL '89 day' and txns.account_created_date + INTERVAL '119 day' then txns.block_timestamp end) as days_active_first_90_120d,
        count(distinct case when txns.block_timestamp between txns.account_created_date + INTERVAL '119 day' and txns.account_created_date + INTERVAL '139 day' then txns.block_timestamp end) as days_active_first_120_150d,
        count(distinct case when txns.block_timestamp between txns.account_created_date + INTERVAL '139 day' and txns.account_created_date + INTERVAL '179 day' then txns.block_timestamp end) as days_active_first_150_180d,
        count(distinct case when txns.block_timestamp between txns.account_created_date + INTERVAL '179 day' and txns.account_created_date + INTERVAL '209 day' then txns.block_timestamp end) as days_active_first_1800_210d,
        count(distinct case when txns.block_timestamp between txns.account_created_date + INTERVAL '209 day' and txns.account_created_date + INTERVAL '239 day' then txns.block_timestamp end) as days_active_first_210_240d,
        count(distinct case when txns.block_timestamp between txns.account_created_date + INTERVAL '239 day' and txns.account_created_date + INTERVAL '269 day' then txns.block_timestamp end) as days_active_first_240_270d,
        count(distinct case when txns.block_timestamp between txns.account_created_date + INTERVAL '269 day' and txns.account_created_date + INTERVAL '299 day' then txns.block_timestamp end) as days_active_first_270_300d,
        count(distinct case when txns.block_timestamp between txns.account_created_date + INTERVAL '299 day' and txns.account_created_date + INTERVAL '329 day' then txns.block_timestamp end) as days_active_first_300_330d,
        count(distinct case when txns.block_timestamp between txns.account_created_date + INTERVAL '329 day' and txns.account_created_date + INTERVAL '359 day' then txns.block_timestamp end) as days_active_first_330_360d,
        count(distinct case when txns.block_timestamp between current_date() - INTERVAL '6 day' and current_date() then txns.block_timestamp end) as days_active_last_7d,
        count(distinct case when txns.block_timestamp between current_date() - INTERVAL '29 day' and current_date() then txns.block_timestamp end) as days_active_last_30d,
        count(distinct case when txns.block_timestamp between current_date() - INTERVAL '59 day' and current_date() - INTERVAL '29 day' then txns.block_timestamp end) as days_active_last_30_60d,
        count(distinct case when txns.block_timestamp between current_date() - INTERVAL '89 day' and current_date() - INTERVAL '59 day' then txns.block_timestamp end) as days_active_last_60_90d
    FROM txns
    group by 1
)
