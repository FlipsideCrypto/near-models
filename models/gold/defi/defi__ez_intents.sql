{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate","block_timestamp::date"],
    unique_key = ['ez_intents_id'],
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,receipt_id);",
    tags = ['intents','curated','scheduled_non_core']
) }}

with
intents as (
    select
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        log_index,
        log_event_index,
        owner_id,
        old_owner_id,
        new_owner_id,
        memo,
        amount_index,
        amount_raw,
        token_id,
        referral,
        dip4_version,
        gas_burnt,
        receipt_succeeded
    from
        {{ ref('defi__fact_intents') }}
    {% if is_incremental() %}
    where modified_timestamp >= (
        select coalesce(max(modified_timestamp),'1970-01-01' :: timestamp)
        from {{ this }}
    )
    {% endif %}
),
native_labels as (
    select
        contract_address,
        name,
        symbol,
        decimals
    from 
        {{ ref('silver__ft_contract_metadata') }}
),
defuse_labels as (
    select
        defuse_asset_identifier,
        asset_name as name,
        decimals
    from 
        {{ ref('silver__defuse_tokens_metadata') }}
)