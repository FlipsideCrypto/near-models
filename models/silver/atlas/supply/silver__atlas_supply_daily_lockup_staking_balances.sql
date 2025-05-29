{{ config(
    materialized = "table",
    cluster_by = ["utc_date"],
    unique_key = "atlas_daily_lockup_staking_balances_id",
    tags = ['scheduled_non_core', 'atlas']
) }}

WITH lockup_receipts AS (

    SELECT
        *
    FROM
        {{ ref('silver__atlas_supply_lockup_receipts') }}
),
dates AS (
    SELECT
        date_day AS utc_date
    FROM
        {{ source(
            'crosschain',
            'dim_dates'
        ) }}
),
lockup_staking_logs AS (
    SELECT
        lr.tx_hash,
        lr.block_timestamp,
        VALUE :FunctionCall :method_name :: STRING AS method_name,
        lr.receiver_id AS lockup_account_id,
        (
            CASE
                method_name
                WHEN 'stake' THEN SPLIT(SPLIT(lr.logs [0], ' at the staking pool') [0], 'Staking ') [1] :: bigint / 1e24
                WHEN 'deposit_and_stake' THEN SPLIT(SPLIT(lr.logs [0], ' to the staking pool') [0], 'Depositing and staking ') [1] :: bigint / 1e24
                WHEN 'unstake' THEN SPLIT(SPLIT(lr.logs [0], ' from the staking pool') [0], 'Unstaking ') [1] :: bigint / 1e24
            END
        ) AS amount,
        lr.logs,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        lockup_receipts AS lr,
        LATERAL FLATTEN(
            input => lr.actions :receipt :Action :actions
        )
    WHERE
        method_name IN (
            'stake',
            'deposit_and_stake',
            'unstake',
            'unstake_all'
        )
),
daily_staking_stats AS (
    SELECT
        lockup_account_id,
        block_timestamp :: DATE AS utc_date,
        SUM(
            CASE
                WHEN method_name IN (
                    'stake',
                    'deposit_and_stake'
                ) THEN amount
                ELSE 0
            END
        ) AS staked_amount_,
        SUM(
            CASE
                WHEN method_name IN ('unstake') THEN amount
                ELSE 0
            END
        ) AS unstaked_amount_,
        (
            CASE
                WHEN COUNT(
                    CASE
                        WHEN method_name = 'unstake_all' THEN tx_hash
                        ELSE NULL
                    END
                ) > 0 THEN TRUE
                ELSE FALSE
            END
        ) AS unstaked_all,
        MIN(_inserted_timestamp) AS _inserted_timestamp
    FROM
        lockup_staking_logs
    GROUP BY
        1,
        2
),
lockup_stakers AS (
    SELECT
        lockup_account_id,
        MIN(block_timestamp) :: DATE AS start_date
    FROM
        lockup_staking_logs
    GROUP BY
        1
),
lockup_stakers_daily_balances__prep_1 AS (
    SELECT
        ls.lockup_account_id,
        d.utc_date
    FROM
        lockup_stakers AS ls,
        dates AS d
    WHERE
        d.utc_date >= ls.start_date
),
lockup_stakers_daily_balances__prep_2 AS (
    SELECT
        d.lockup_account_id,
        d.utc_date,
        COALESCE(
            dss.staked_amount_,
            0
        ) AS staked_amount,
        COALESCE(
            dss.unstaked_amount_,
            0
        ) AS unstaked_amount,
        dss.unstaked_all,
        SUM(
            CASE
                WHEN dss.unstaked_all = TRUE THEN 1
                ELSE 0
            END
        ) over (
            PARTITION BY d.lockup_account_id
            ORDER BY
                d.utc_date rows BETWEEN unbounded preceding
                AND CURRENT ROW
        ) AS _unstake_counter,
        dss._inserted_timestamp
    FROM
        lockup_stakers_daily_balances__prep_1 AS d
        LEFT JOIN daily_staking_stats AS dss
        ON dss.lockup_account_id = d.lockup_account_id
        AND dss.utc_date = d.utc_date
),
lockup_stakers_daily_balances__prep_3 AS (
    SELECT
        *,
        COALESCE(LAG(_unstake_counter) over (PARTITION BY lockup_account_id
    ORDER BY
        utc_date), 0) AS staking_period_index
    FROM
        lockup_stakers_daily_balances__prep_2
),
lockup_stakers_daily_balances__prep_4 AS (
    SELECT
        *,
        SUM(
            staked_amount - unstaked_amount
        ) over (
            PARTITION BY lockup_account_id,
            staking_period_index
            ORDER BY
                utc_date rows BETWEEN unbounded preceding
                AND CURRENT ROW
        ) AS _cumulative_staked_amount,
        (
            CASE
                WHEN unstaked_all = TRUE THEN 0
                ELSE _cumulative_staked_amount
            END
        ) AS staked_balance
    FROM
        lockup_stakers_daily_balances__prep_3
),
lockup_stakers_daily_balances AS (
    SELECT
        lockup_account_id,
        utc_date,
        staked_balance,
        _inserted_timestamp
    FROM
        lockup_stakers_daily_balances__prep_4
)
SELECT
    lockup_account_id,
    utc_date,
    staked_balance,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['lockup_account_id', 'utc_date']) }} AS atlas_daily_lockup_staking_balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    lockup_stakers_daily_balances
