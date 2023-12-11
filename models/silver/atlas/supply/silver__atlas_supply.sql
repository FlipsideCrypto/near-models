{{ config(
    materialized = "table",
    cluster_by = ["utc_date"],
    unique_key = "atlas_supply_id",
    tags = ['atlas']
) }}

WITH daily_lockup_locked_balances AS (

    SELECT
        *
    FROM
        {{ ref('silver__atlas_supply_daily_lockup_locked_balances') }}
),
daily_lockup_staking_balances AS (
    SELECT
        *
    FROM
        {{ ref('silver__atlas_supply_daily_lockup_staking_balances') }}
),
daily_lockup_locked_and_staking_balances AS (
    SELECT
        l.lockup_account_id,
        l.utc_date,
        l.locked_amount,
        COALESCE(
            s.staked_balance,
            0
        ) AS staked_amount,
        LEAST(
            staked_amount,
            locked_amount
        ) AS locked_and_staked_amount
    FROM
        daily_lockup_locked_balances AS l
        LEFT JOIN daily_lockup_staking_balances AS s
        ON s.lockup_account_id = l.lockup_account_id
        AND s.utc_date = l.utc_date
),
daily_locked_and_staked_supply AS (
    SELECT
        utc_date,
        SUM(locked_amount) AS total_locked_supply,
        SUM(locked_and_staked_amount) AS locked_and_staked_supply
    FROM
        daily_lockup_locked_and_staking_balances
    GROUP BY
        1
),
daily_staked_supply AS (
    SELECT
        date_day AS utc_date,
        SUM(balance) AS total_staked_supply
    FROM
        {{ ref('silver__pool_balance_daily') }}
    GROUP BY
        1
),
daily_total_supply AS (
    SELECT
        end_time :: DATE AS utc_date,
        total_near_supply AS total_supply
    FROM
        {{ ref('silver__atlas_supply_epochs') }}
        qualify ROW_NUMBER() over (
            PARTITION BY end_time :: DATE
            ORDER BY
                end_time DESC
        ) = 1
),
daily_supply_stats AS (
    SELECT
        s.utc_date,
        ts.total_supply,
        s.total_staked_supply,
        ts.total_supply - s.total_staked_supply AS total_nonstaked_supply,
        ls.total_locked_supply,
        ls.locked_and_staked_supply,
        GREATEST(
            0,
            total_staked_supply - locked_and_staked_supply
        ) AS nonlocked_and_staked_supply,
        GREATEST(
            0,
            total_locked_supply - locked_and_staked_supply
        ) AS locked_and_nonstaked_supply,
        total_supply - locked_and_staked_supply - locked_and_nonstaked_supply - nonlocked_and_staked_supply AS nonlocked_and_nonstaked_supply,
        total_supply - total_locked_supply AS circulating_supply,
        total_locked_supply AS locked_supply
    FROM
        daily_staked_supply AS s
        LEFT JOIN daily_locked_and_staked_supply AS ls
        ON ls.utc_date = s.utc_date
        LEFT JOIN daily_total_supply AS ts
        ON ts.utc_date = s.utc_date
),
output AS (
    SELECT
        utc_date,
        total_supply,
        total_staked_supply,
        total_nonstaked_supply,
        circulating_supply,
        total_locked_supply,
        nonlocked_and_nonstaked_supply AS liquid_supply,
        total_supply - nonlocked_and_nonstaked_supply AS nonliquid_supply,
        locked_and_staked_supply AS staked_locked_supply,
        locked_and_nonstaked_supply AS non_staked_locked_supply,
        nonlocked_and_staked_supply AS staked_circulating_supply,
        nonlocked_and_nonstaked_supply AS nonstaked_circulating_supply,
        total_locked_supply / total_supply AS perc_locked_supply,
        circulating_supply / total_supply AS perc_circulating_supply,
        locked_and_staked_supply / total_locked_supply AS perc_staked_locked,
        nonlocked_and_staked_supply / circulating_supply AS perc_staked_circulating
    FROM
        daily_supply_stats
)
SELECT
    utc_date,
    total_supply,
    total_staked_supply,
    total_nonstaked_supply,
    circulating_supply,
    total_locked_supply,
    liquid_supply,
    nonliquid_supply,
    staked_locked_supply,
    non_staked_locked_supply,
    staked_circulating_supply,
    nonstaked_circulating_supply,
    perc_locked_supply,
    perc_circulating_supply,
    perc_staked_locked,
    perc_staked_circulating,
    {{ dbt_utils.generate_surrogate_key(['utc_date']) }} AS atlas_supply_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    output
