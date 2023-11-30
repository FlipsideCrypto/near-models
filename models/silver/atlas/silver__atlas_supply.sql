{{ config(
    materialized = "incremental",
    cluster_by = ["utc_date"],
    unique_key = "atlas_supply_id",
    merge_exclude_columns = ["inserted_timestamp"],
    incremental_strategy = "merge",
    tags = ['atlas']
) }}

WITH 
daily_lockup_locked_balances AS (
    select * from {{ ref('silver__atlas_supply_daily_lockup_locked_balances')}}
),
daily_lockup_staking_balances AS (
select * from {{ ref('silver__atlas_supply_daily_lockup_staking_balances')}}
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
select * from {{ ref('silver__atlas_supply_daily_staked_supply')}}
),
daily_supply_stats AS (
    SELECT
        s.utc_date,
        s.total_supply,
        s.total_staked_supply,
        s.total_supply - s.total_staked_supply AS total_nonstaked_supply,
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
),
output AS (
    SELECT
        utc_date,
        utc_date AS "Date",
        total_supply AS "Total Supply - Actual",
        total_staked_supply AS "Staked Supply",
        total_nonstaked_supply AS "Non-staked Supply",
        circulating_supply AS "Circulating Supply",
        total_supply - circulating_supply AS "Total Supply",
        total_locked_supply AS "Locked Supply",
        nonlocked_and_nonstaked_supply AS "Liquid Supply",
        total_supply - nonlocked_and_nonstaked_supply AS "Non-liquid Supply",
        locked_and_staked_supply AS "Staked (Locked Supply)",
        locked_and_nonstaked_supply AS "Non-staked (Locked Supply)",
        nonlocked_and_staked_supply AS "Staked (Circulating Supply)",
        nonlocked_and_nonstaked_supply AS "Non-staked (Circulating Supply)",
        total_locked_supply / total_supply AS perc_locked_supply,
        circulating_supply / total_supply AS perc_circulating_supply,
        locked_and_staked_supply / total_locked_supply AS perc_staked__locked,
        nonlocked_and_staked_supply / circulating_supply AS perc_staked__circulating,
        1 AS dummy
    FROM
        daily_supply_stats
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['utc_date']) }} AS atlas_supply_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    output
