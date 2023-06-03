{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    cluster_by = ['_partition_by_block_number', 'block_timestamp::date'],
    unique_key = 'signer_account_id',
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
    LIMIT 20000000
),

actions_methods as (
    SELECT  
        a.signer_id,
        a.receiver_id,
        a.tx_hash,
        nvl(action_data:method_name::string, 'no_method') as method_name,
        _load_timestamp,
        block_timestamp,
        _partition_by_block_number,
        a.action_name,
        CASE WHEN a.action_name = 'FunctionCall' THEN 'FUNCTION_CALL'
            WHEN a.action_name = 'AddKey' THEN 'ADD_KEY'
            WHEN a.action_name = 'Transfer' THEN 'TRANSFER'
            ELSE 'no_action' END as action_kind,             
        CASE WHEN action_kind = 'TRANSFER' THEN transaction_hash ELSE NULL END as total_transfers_done,
        CASE WHEN action_kind = 'ADD_KEY' THEN transaction_hash ELSE NULL END as keys_added,
        CASE WHEN action_kind = 'FUNCTION_CALL' THEN method_name ELSE NULL END as total_methods_called,

        CASE WHEN lower(method_name) LIKE '%nft%' or lower(method_name) LIKE '%mint%' THEN method_name ELSE NULL END as approx_nft_txns,
        CASE WHEN lower(method_name) LIKE '%nft%' and lower(method_name) LIKE '%mint%' THEN method_name ELSE NULL END as nft_mints
    FROM fact_actions_events a
),

actions_methods_count as(
   SELECT
       signer_account_id,
       count(distinct method_name) as total_actions_taken, 
       count(distinct  total_transfers_done) as total_transfers_done,
       count(distinct keys_added) as keys_added,
       count(distinct total_methods_called) as total_methods_called,
       count(distinct approx_nft_txns) as approx_nft_txns,
       count(distinct nft_mints) as nft_mints
   FROM actions_methods
   GROUP BY signer_account_id
   // Group by block_timestamp::date truncate and signer_account_id
),

txs_metrics as (
   SELECT
       //a.block_timestamp,
       a.signer_id as signer_account_id,
       min(a.block_timestamp) as account_created_date,
       max(a.block_timestamp) as last_transaction,  
       count(distinct a.receiver_id) as accounts_transacted_with,
       count(distinct a.tx_hash) as total_txns,
       count(distinct a.block_timestamp) as total_days_active
   FROM fact_actions_events as a
   GROUP BY signer_account_id
   // Group by block_timestamp::date truncate and signer_account_id
),
activity_metrics as (
    SELECT 
        actions.*,
        txns.account_created_date,
        txns.last_transaction,
        txns.total_txns,
        txns.total_days_active
    FROM
        actions_methods_count as actions
    JOIN txs_metrics as txns
    ON actions.signer_account_id = txns.signer_account_id
)

SELECT * FROM activity_metrics



-- 2. ACTIVITY METRICS
--   2a. txs metrics 
--     - min/max block timestamp
--     - days activy (count block ts)
--     - coutn txs, etc.

--   2b. actions metrics
--     - count of methods called by signers
--       - *note - check signer of receipt vs transaction!

--   join those 2 ^ on signer id to get 
--   - signer w basic activity metrics 