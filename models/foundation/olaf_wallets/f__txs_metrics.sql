{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['signer_id'],
    unique_key = 'signer_id',
    tags = ['actions', 'olap']
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

last_update AS (
    SELECT
        *
    FROM
        {{ ref('f__last_update') }}
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

account_counter as (
    SELECT
        a.signer_id,
        count(distinct a.receiver_id) as accounts_transacted_with,
        count(distinct a.tx_hash) as total_txns,
        count(distinct a.block_timestamp) as total_days_active
    FROM fact_actions_events as a
    GROUP BY 1
)

select 
    a.signer_id,
    a.accounts_transacted_with,
    a.total_txns,
    a.total_days_active,
    lu.last_transaction,  
    acc_owner.block_timestamp as account_created_date
from account_counter as a
LEFT JOIN last_update as lu ON a.signer_id = lu.signer_id
LEFT JOIN account_owner as acc_owner on a.signer_id = acc_owner.receiver_id

// time 1 minutes and 58.08 seconds