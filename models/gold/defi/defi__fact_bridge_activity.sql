{{ config(
    materialized = 'view',
    secure = false,
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, BRIDGING' }} },
    tags = ['scheduled_non_core']
) }}

WITH 
rainbow AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw,
        amount_adj,
        destination_address,
        source_address,
        platform,
        bridge_address,
        destination_chain_id AS destination_chain,
        source_chain_id AS source_chain,
        method_name,
        direction,
        receipt_succeeded,
        bridge_rainbow_id AS fact_bridge_activity_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__bridge_rainbow') }}
),
wormhole AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw,
        amount_adj,
        destination_address,
        source_address,
        platform,
        bridge_address,
        id.blockchain AS destination_chain,
        id2.blockchain AS source_chain,
        method_name,
        direction,
        receipt_succeeded,
        bridge_wormhole_id AS fact_bridge_activity_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__bridge_wormhole') }} b
    LEFT JOIN {{ ref('seeds__wormhole_ids') }} id ON b.destination_chain_id = id.id
    LEFT JOIN {{ ref('seeds__wormhole_ids') }} id2 ON b.source_chain_id = id2.id
),
multichain AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw,
        amount_adj,
        destination_address,
        source_address,
        platform,
        bridge_address,
        id.blockchain AS destination_chain,
        id2.blockchain AS source_chain,
        method_name,
        direction,
        receipt_succeeded,
        bridge_multichain_id AS fact_bridge_activity_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__bridge_multichain') }} b
    LEFT JOIN {{ ref('seeds__multichain_ids') }} id ON b.destination_chain_id = id.id
    LEFT JOIN {{ ref('seeds__multichain_ids') }} id2 ON b.source_chain_id = id2.id
),
allbridge AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw,
        amount_adj,
        destination_address,
        source_address,
        platform,
        bridge_address,
        id.blockchain AS destination_chain,
        id2.blockchain AS source_chain,
        method_name,
        direction,
        receipt_succeeded,
        bridge_allbridge_id AS fact_bridge_activity_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__bridge_allbridge') }} b
    LEFT JOIN {{ ref('seeds__allbridge_ids') }} id ON b.destination_chain_id = id.id
    LEFT JOIN {{ ref('seeds__allbridge_ids') }} id2 ON b.source_chain_id = id2.id
),
FINAL AS (
    SELECT
        *
    FROM
        rainbow
    UNION ALL
    SELECT
        *
    FROM
        wormhole
    UNION ALL
    SELECT
        *
    FROM
        multichain
    UNION ALL
    SELECT
        *
    FROM
        allbridge
)
SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw AS amount_unadj,
        amount_adj,
        destination_address,
        source_address,
        platform,
        bridge_address,
        LOWER(destination_chain) AS destination_chain,
        LOWER(source_chain) AS source_chain,
        method_name,
        direction,
        receipt_succeeded,
        fact_bridge_activity_id,
        inserted_timestamp,
        modified_timestamp
FROM
    FINAL
