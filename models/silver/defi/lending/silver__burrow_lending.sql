{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core']
) }}


WITH metadata  AS (
    SELECT
        asset_identifier as contract_address,
        NAME,
        symbol,
        decimals
    FROM
        {{ ref('silver__ft_contract_metadata') }}
),
borrows AS
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
FINAL_UNION AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(
        ['burrow_borrows_id', 'actions']
        ) }} as  burrow_lending_id,
        *
    FROM
        borrows
    UNION ALL
    SELECT
        {{ dbt_utils.generate_surrogate_key(
        ['burrow_collaterals_id', 'actions']
        ) }} as  burrow_lending_id,
        *
    FROM
        collaterals
    UNION ALL
    SELECT
        {{ dbt_utils.generate_surrogate_key(
        ['burrow_deposits_id', 'actions']
        ) }} as  burrow_lending_id,
        *
    FROM
        deposits
    UNION ALL
    SELECT
        {{ dbt_utils.generate_surrogate_key(
        ['burrow_repays_id', 'actions']
        ) }} as  burrow_lending_id,
        *
    FROM
        repays
    UNION ALL
    SELECT
        {{ dbt_utils.generate_surrogate_key(
        ['burrow_withdraws_id', 'actions']
        ) }} as  burrow_lending_id,
        *
    FROM
        withdrawals
),
FINAL AS (
    SELECT
        'burrow' as platform,
        tx_hash,
        block_id,
        block_timestamp,
        sender_id,
        actions,
        f.contract_address,
        amount_raw,
        GREATEST(
            amount_raw,
            RPAD(
                amount_raw,
                m.decimals,
                '0'
        )) :: NUMBER AS amount_adj,
        burrow_lending_id,
        token_contract_address,
        inserted_timestamp,
        modified_timestamp
    FROM
        FINAL_UNION as f
    LEFT JOIN metadata m ON
        token_contract_address = m.contract_address

)
SELECT
    'burrow' as platform,
    tx_hash,
    block_id,
    block_timestamp,
    sender_id,
    actions,
    contract_address,
    amount_raw,
    amount_adj,
    burrow_lending_id, 
    token_contract_address as token_address,
    inserted_timestamp,
    modified_timestamp
FROM
    FINAL
