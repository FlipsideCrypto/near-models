{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'signer_account_id + account_creator',
    cluster_by = ['block_timestamp::DATE', '_load_timestamp::DATE'],
    tags = ['olaf']
) }}


WITH receipts AS (

    SELECT
        *
    FROM
        {{ ref('silver__olaf_receipts') }}
    WHERE
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),

txns AS (

    SELECT
        *
    FROM
        {{ ref('silver__olaf_txns') }}
    WHERE
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
)


select
    txns.signer_account_id,
    r.account_creator,
    txns.receiver_account_id,
    txns.block_timestamp,
    txns.transaction_hash,
    txns.method_name,
    txns.total_transfers_done,
    txns.keys_added,
    txns.total_methods_called,
    txns.approx_nft_a,
    txns.nft_mints,
    txns._LOAD_TIMESTAMP as _load_timestamp
from txns
  left join receipts r on txns.signer_account_id = r.receiver_account_id




    -- --b.wallet_balance,
    -- --b.block_date as balance_updated_on,   
    -- count(distinct txns.receiver_account_id) as accounts_transacted_with,
    -- --sum(txns.gas_fee) as gas_paid,
    -- min(txns.block_timestamp) as account_created_date,
    -- max(txns.block_timestamp) as last_transaction,
    -- count(distinct txns.transaction_hash) as total_txns,
    -- count(distinct txns.block_timestamp) as total_days_active,
    -- count(distinct txns.method_name) as total_actions_taken,
    -- count(distinct txns.total_transfers_done) as total_transfers_done,
    -- count(distinct txns.keys_added) as keys_added,
    -- count(txns.total_methods_called) as total_methods_called,
    -- count(txns.approx_nft_a) as approx_nft_txns,
    -- count(txns.nft_mints) as nft_mints

    -- group by 1, 2 