with mints as (
    select * 
    from {{ ref('silver__nft_mints') }}
)

select
    action_id,
    tx_hash,
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
    nft_address,
    network_fee,
    tx_status
from mints
