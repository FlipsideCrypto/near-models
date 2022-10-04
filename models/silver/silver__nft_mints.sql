{{
    config(
        materialized="incremental",
        cluster_by=["block_timestamp::DATE", "_inserted_timestamp::DATE"],
        unique_key="action_id",
        incremental_strategy="delete+insert",
    )
}}

-- data from silver_action_event table
with
    nft_mint as (
        select
            action_id,
            tx_hash,
            block_id,
            block_timestamp,
            action_index,
            action_name,
            action_data:method_name::string as method_name,
            _ingested_at,
            _inserted_timestamp
        from {{ ref("silver__actions_events") }}

        where
            method_name in ('nft_mint', 'nft_mint_batch')
            and {{ incremental_load_filter("_unserted_timestamp") }}

    -- Data pulled from action_events_function_call
    ),
    function_call_data as (
        select
            action_id,
            tx_hash,
            block_id,
            block_timestamp,
            try_parse_json(args) as args_json,
            method_name,
            deposit / pow(10, 24) as deposit,
            attached_gas,
            case
                when args_json:receiver_id is not null
                then args_json:receiver_id::string
                when args_json:receiver_ids is not null
                then args_json:receiver_ids::string
            end as project_name,
            case
                when args_json:token_series_id is not null
                then try_parse_json(args_json:token_series_id)::string
                when args_json:token_owner_id is not null
                then try_parse_json(args_json:token_series_id)::string
            end as nft_id,
            try_parse_json(args_json:token_id)::string as token_id


        from {{ ref("silver__actions_events_function_call") }}
        where
            method_name in ('nft_mint', 'nft_mint_batch')
            and tx_hash in (select distinct tx_hash from nft_mint)

    -- Data Pulled from Transaction 
    ),
    mint_transactions as (
        select
            tx_hash,
            tx_signer as signer,
            tx_receiver as receiver,
            transaction_fee as network_fee,
            gas_used,
            attached_gas,
            tx_status,
            tx:actions[0]:functioncall:method_name::string as method_name

        from {{ ref("silver__transactions") }}
        where
            tx_hash in (select distinct tx_hash from nft_mint) and tx_status = 'Success'

    -- Data pulled from Receipts Table
    ),
    receipts_data as (
        select

            tx_hash,
            receipt_index,
            receipt_object_id as receipt_id,
            receipt_outcome_id::string as receipt_outcome_id,
            receiver_id,
            gas_burnt
        from {{ ref("silver__receipts") }}
        where tx_hash in (select distinct tx_hash from nft_mint)

    )


select distinct
    nft_mint.action_id,
    nft_mint.tx_hash,
    nft_mint.block_id,
    nft_mint.block_timestamp,
    nft_mint.method_name,
    nft_mint._ingested_at,
    nft_mint._inserted_timestamp,
    signer,
    receiver,
    project_name,
    token_id,
    nft_id,
    receipts_data.receiver_id as nft_address,
    network_fee,
    tx_status

from nft_mint
left join function_call_data on nft_mint.tx_hash = function_call_data.tx_hash
left join mint_transactions on nft_mint.tx_hash = mint_transactions.tx_hash
left join receipts_data on nft_mint.tx_hash = receipts_data.tx_hash
where tx_status is not null
