{{
    config(
        materialized="incremental",
        cluster_by=["block_timestamp::DATE", "_inserted_timestamp::DATE"],
        unique_key="action_id",
        incremental_strategy="delete+insert",
    tags = ['curated']

    )
}}

--Data pulled from action_events_function_call 
with
    function_call as (
        select
            action_id,
            tx_hash,
            block_id,
            block_timestamp,
            try_parse_json(args) as args_json,
            method_name,
            deposit / pow(10, 24) as deposit,
            _ingested_at,
            _inserted_timestamp, 
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
            and {{ incremental_load_filter("_inserted_timestamp") }}
    ),

    --Data Pulled from Transaction
    mint_transactions as (
        select
            tx_hash,
            tx_signer,
            tx_receiver,
            transaction_fee / pow(10, 24) as network_fee,
            tx_status
        -- tx:actions[0]:functioncall:method_name::string as method_name
        from {{ ref("silver__transactions") }}
        where
            tx_hash in (select distinct tx_hash from function_call)
            and tx_status = 'Success'
            and {{ incremental_load_filter("_inserted_timestamp") }}


    ),
    --Data pulled from Receipts Table
    receipts_data as (
        select

            tx_hash,
            receipt_index,
            receipt_object_id as receipt_id,
            receipt_outcome_id::string as receipt_outcome_id,
            receiver_id,
            gas_burnt
        from {{ ref("silver__receipts") }}
        where
            tx_hash in (select distinct tx_hash from function_call)
            and {{ incremental_load_filter("_inserted_timestamp") }}

    )


select distinct
    action_id,
    function_call.tx_hash,
    block_id,
    block_timestamp,
    method_name,
    _ingested_at,
    _inserted_timestamp,
    tx_signer,
    tx_receiver,
    project_name,
    token_id,
    nft_id,
    receipts_data.receiver_id as nft_address,
    network_fee,
    tx_status

from function_call
left join mint_transactions on function_call.tx_hash = mint_transactions.tx_hash
left join receipts_data on function_call.tx_hash = receipts_data.tx_hash
where tx_status is not null

