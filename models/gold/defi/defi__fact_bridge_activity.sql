{{ config(
    materialized = 'view',
    secure = false,
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, BRIDGING' }} },
    tags = ['core']
) }}

WITH rainbow AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw,
        destination_address,
        source_address,
        platform,
        destination_chain_id,
        source_chain_id,
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
        destination_address,
        source_address,
        platform,
        destination_chain_id,
        source_chain_id,
        receipt_succeeded,
        bridge_wormhole_id AS fact_bridge_activity_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__bridge_wormhole') }}
),
multichain AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw,
        destination_address,
        source_address,
        platform,
        destination_chain_id,
        source_chain_id,
        receipt_succeeded,
        bridge_multichain_id AS fact_bridge_activity_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__bridge_multichain') }}
),
allbridge AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw,
        destination_address,
        source_address,
        platform,
        destination_chain_id,
        source_chain_id,
        receipt_succeeded,
        bridge_allbridge_id AS fact_bridge_activity_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__bridge_allbridge') }}
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
)
SELECT
    *
FROM
    FINAL
