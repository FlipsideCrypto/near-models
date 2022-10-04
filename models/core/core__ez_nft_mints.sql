with mints as (select * from {{ ref("silver__nft_mints") }})

select
    action_id,
    tx_hash,
    nft_mint.block_id,
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
from mints
