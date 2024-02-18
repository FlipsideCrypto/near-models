{{ config(
    materialized = 'view',
    secure = false,
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, BRIDGING' }} },
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
repay AS
(
    SELECT
        *
    FROM
        {{ ref('silver__burrow_repay') }}
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
        *
    FROM
        borrows
    UNION ALL
    SELECT
        *
    FROM
        collaterals
    UNION ALL
    SELECT
        *
    FROM
        deposits
    UNION ALL
    SELECT
        *
    FROM
        repay
    UNION ALL
    SELECT
        *
    FROM
        withdrawals
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    sender,
    actions,
    contract_address,
    amount,
    token_contract_address
FROM
    FINAL