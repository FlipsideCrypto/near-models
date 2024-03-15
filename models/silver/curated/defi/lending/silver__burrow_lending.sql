{{ config(
    materialized = 'view',
    tags = ['core']
) }}


WITH borrows AS
(
    SELECT
        *
    FROM
        {{ ref('silver__burrow_borrows') }}

),
collaterals AS
(
    SELECT
        *
    FROM
        {{ ref('silver__burrow_collaterals') }}
),
deposits AS
(
    SELECT
        *
    FROM
        {{ ref('silver__burrow_deposits') }}

),
repays AS
(
    SELECT
        *
    FROM
        {{ ref('silver__burrow_repays') }}
),
withdrawals AS
(
    SELECT
        *
    FROM
        {{ ref('silver__burrow_withdraws') }}

),
FINAL AS (
    SELECT
        burrow_borrows_id as  burrow_lending_id,
        *
    FROM
        borrows
    UNION ALL
    SELECT
        burrow_collaterals_id as  burrow_lending_id,
        *
    FROM
        collaterals
    UNION ALL
    SELECT
        burrow_deposits_id as  burrow_lending_id,
        *
    FROM
        deposits
    UNION ALL
    SELECT
        burrow_repays_id as  burrow_lending_id,
        *
    FROM
        repays
    UNION ALL
    SELECT
        burrow_withdraws_id as  burrow_lending_id,
        *
    FROM
        withdrawals
)
SELECT
    'burrow' as platform,
    tx_hash,
    block_id,
    block_timestamp,
    sender_id,
    actions,
    contract_address,
    amount,
    burrow_lending_id,
    token_contract_address as token_address,
    inserted_timestamp,
    modified_timestamp
FROM
    FINAL