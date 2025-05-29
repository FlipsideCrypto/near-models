{{ config(
    materialized = "table",
    cluster_by = ["utc_date"],
    unique_key = "atlas_daily_lockup_locked_balances_id",
    tags = ['scheduled_non_core', 'atlas']
) }}

WITH lockup_receipts AS (
    SELECT
        *
    FROM
        {{ ref('silver__atlas_supply_lockup_receipts') }}
),
function_call AS (
    SELECT
        tx_hash,
        block_timestamp,
        receipt_id,
        action_index,
        receipt_receiver_id AS receiver_id,
        action_data :method_name ::STRING AS method_name,
        action_data :args ::VARIANT AS args,
        receipt_succeeded
    FROM
        {{ ref('core__ez_actions') }}
    WHERE
        action_name = 'FunctionCall'
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
new_lockup_txs AS (
    -- new lockup contract created
    SELECT
        receipt_object_id, -- slated for rename to receipt_id
        tx_hash,
        block_timestamp,
        receiver_id AS lockup_account_id,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        lockup_receipts,
        LATERAL FLATTEN(
            input => actions :receipt :Action :actions
        )
    WHERE
        VALUE :FunctionCall :method_name :: STRING = 'new'
),
terminate_vesting_txs AS (
    -- vesting is stopped by the Foundation
    SELECT
        tx_hash,
        block_timestamp,
        receiver_id AS lockup_account_id,
        SPLIT(
            logs [0],
            'unvested balance is '
        ) [1] :: bigint / 1e24 AS unvested_balance,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        lockup_receipts,
        LATERAL FLATTEN(
            input => actions :receipt :Action :actions
        )
    WHERE
        VALUE :FunctionCall :method_name :: STRING = 'terminate_vesting'
),
terminate_vesting_txs_with_vesting_schedule AS (
    SELECT
        tv.*,
        fc.args :vesting_schedule_with_salt :vesting_schedule AS vesting_schedule,
        tv._inserted_timestamp,
        tv._partition_by_block_number
    FROM
        terminate_vesting_txs AS tv
        LEFT JOIN function_call AS fc
        ON fc.tx_hash = tv.tx_hash
        AND fc.method_name = 'terminate_vesting' qualify ROW_NUMBER() over (
            PARTITION BY tv.tx_hash
            ORDER BY
                tv.block_timestamp
        ) = 1 -- dedupe
),
-- unvested tokens are withdrawn (effectively unlocked into circulating supply)
termination_withdraw_txs AS (
    SELECT
        tx_hash,
        block_timestamp,
        receiver_id AS lockup_account_id,
        SPLIT(
            SPLIT(
                logs [0],
                ' of terminated unvested balance'
            ) [0],
            'Withdrawing '
        ) [1] :: bigint / 1e24 AS withdrawn_amount,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        lockup_receipts,
        LATERAL FLATTEN(
            input => actions :receipt :Action :actions
        )
    WHERE
        VALUE :FunctionCall :method_name :: STRING = 'termination_withdraw' -- Simplify logic -> get only first termination withdrawal
        -- QUALIFY row_number() OVER (partition by lockup_account_id order by block_timestamp) = 1
),
daily_termination_withdrawn_amount AS (
    SELECT
        lockup_account_id,
        block_timestamp :: DATE AS utc_date,
        SUM(withdrawn_amount) AS withdrawn_amount
    FROM
        termination_withdraw_txs
    GROUP BY
        1,
        2
),
-- lockup amounts
deposits AS (
    SELECT
        tx_hash,
        block_timestamp,
        VALUE :Transfer :deposit :: bigint / 1e24 AS deposit_amount,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        lockup_receipts,
        LATERAL FLATTEN(
            input => actions :receipt :Action :actions
        )
    WHERE
        object_keys(
            VALUE :Transfer
        ) [0] :: STRING = 'deposit'
),
lockup_contracts AS (
    SELECT
        lcr.tx_hash,
        lcr.block_timestamp,
        lcr.lockup_account_id,
        fc1.args :lockup_duration :: bigint AS input_lockup_duration_ns,
        fc1.args :lockup_timestamp :: bigint AS input_lockup_timestamp_epoch,
        fc1.args :owner_account_id :: STRING AS owner_account_id,
        fc1.args :release_duration :: bigint AS input_release_duration_ns,
        COALESCE(
            tv.vesting_schedule,
            fc1.args :vesting_schedule :VestingSchedule
        ) AS vesting_schedule_,
        vesting_schedule_ :cliff_timestamp :: bigint AS vesting_cliff_timestamp_epoch,
        vesting_schedule_ :start_timestamp :: bigint AS vesting_start_timestamp_epoch,
        vesting_schedule_ :end_timestamp :: bigint AS vesting_end_timestamp_epoch,
        COALESCE(
            fc1.args :transfers_information :TransfersEnabled :transfers_timestamp :: bigint,
            1602614338293769340
        ) AS transfers_enabled_timestamp_epoch,
        d.deposit_amount,
        tv.block_timestamp AS terminate_vesting_timestamp,
        tv.unvested_balance AS termination_unvested_amount,
        tw.block_timestamp AS termination_withdraw_timestamp,
        tw.withdrawn_amount AS termination_withdrawn_amount,
        (
            CASE
                WHEN object_keys(
                    fc1.args :vesting_schedule
                ) [0] :: STRING = 'VestingHash' THEN TRUE
                ELSE FALSE
            END
        ) AS is_private_vesting,
        lcr._inserted_timestamp,
        lcr._partition_by_block_number
    FROM
        new_lockup_txs AS lcr
        LEFT JOIN function_call AS fc1
        ON fc1.tx_hash = lcr.tx_hash
        AND fc1.method_name = 'new'
        LEFT JOIN deposits AS d
        ON d.tx_hash = lcr.tx_hash
        LEFT JOIN terminate_vesting_txs_with_vesting_schedule AS tv
        ON tv.lockup_account_id = lcr.lockup_account_id
        LEFT JOIN termination_withdraw_txs AS tw
        ON tw.lockup_account_id = lcr.lockup_account_id
    WHERE
        lcr.tx_hash IN (
            SELECT
                tx_hash
            FROM
                new_lockup_txs
        )
        AND d.deposit_amount > 0
),
lockup_contracts__parsed AS (
    SELECT
        lockup_account_id,
        -- the number of times the same lockup account ID has been used (used as part of lockup unique identifier)
        ROW_NUMBER() over (
            PARTITION BY lockup_account_id
            ORDER BY
                block_timestamp
        ) AS lockup_index,
        owner_account_id,
        deposit_amount AS lockup_amount,
        -- timestamp when tokens were locked (lock start)
        block_timestamp AS deposit_timestamp,
        -- timestamp when transfers were enabled in the blockchain (default reference when lockup_timestamp is null)
        TO_TIMESTAMP(
            transfers_enabled_timestamp_epoch,
            9
        ) AS transfers_enabled_timestamp,
        -- timestamp when tokens start unlocking (explicit parameter)
        TO_TIMESTAMP(
            input_lockup_timestamp_epoch,
            9
        ) AS input_lockup_timestamp,
        -- if lockup_timestamp is null, calculate unlock start from lockup duration
        TIMESTAMPADD(
            nanoseconds,
            input_lockup_duration_ns,
            transfers_enabled_timestamp
        ) AS calculated_lockup_timestamp,
        -- lockup mechanism
        input_lockup_duration_ns,
        input_release_duration_ns,
        -- Max between input and calculated lockup timestamp
        (
            CASE
                WHEN input_lockup_timestamp IS NOT NULL THEN GREATEST(
                    input_lockup_timestamp,
                    calculated_lockup_timestamp
                )
                ELSE calculated_lockup_timestamp
            END
        ) AS lockup_timestamp,
        -- If release_duration is not provided, tokens are immediately unlocked
        (
            CASE
                WHEN input_release_duration_ns IS NOT NULL THEN TIMESTAMPADD(
                    nanosecond,
                    input_release_duration_ns,
                    lockup_timestamp
                ) -- linear release if release_duration is provided, else full unlock
                ELSE lockup_timestamp
            END
        ) AS lockup_end_timestamp,
        -- vesting mechanism
        is_private_vesting,
        TO_TIMESTAMP(
            vesting_start_timestamp_epoch,
            9
        ) AS vesting_start_timestamp,
        TO_TIMESTAMP(
            vesting_end_timestamp_epoch,
            9
        ) AS vesting_end_timestamp,
        TO_TIMESTAMP(
            vesting_cliff_timestamp_epoch,
            9
        ) AS vesting_cliff_timestamp,
        -- vesting termination
        terminate_vesting_timestamp,
        termination_unvested_amount,
        termination_withdraw_timestamp,
        termination_withdrawn_amount,
        tx_hash AS _tx_hash,
        (
            CASE
                WHEN lockup_timestamp IS NOT NULL
                AND vesting_start_timestamp IS NULL THEN LEAST(
                    deposit_timestamp,
                    lockup_timestamp
                )
                WHEN lockup_timestamp IS NULL
                AND vesting_start_timestamp IS NOT NULL THEN LEAST(
                    deposit_timestamp,
                    vesting_start_timestamp
                )
                ELSE LEAST(
                    deposit_timestamp,
                    lockup_timestamp,
                    vesting_start_timestamp
                )
            END
        ) :: DATE AS _lockup_start_date,
        (
            CASE
                WHEN lockup_end_timestamp IS NOT NULL
                AND vesting_end_timestamp IS NULL THEN lockup_end_timestamp
                WHEN lockup_end_timestamp IS NULL
                AND vesting_end_timestamp IS NOT NULL THEN vesting_end_timestamp
                ELSE GREATEST(
                    lockup_end_timestamp,
                    vesting_end_timestamp
                )
            END
        ) :: DATE AS _lockup_end_date,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        lockup_contracts
),
{# TODO - above might be 1 table, and below aggregation another one. Below CTS is where the cross join with dates is happening, calculating the time remaining and resulting balance(s) #}
lockup_contracts_daily_balance__prep_1 AS (
    SELECT
        lc.lockup_account_id,
        lc.lockup_index,
        lc.owner_account_id,
        d.utc_date,
        d.utc_date + INTERVAL '1 day' - INTERVAL '1 nanosecond' AS block_timestamp,
        -- End of day block timestamp
        lc.lockup_amount,
        lc.deposit_timestamp,
        -- Lockup logic
        lc.lockup_timestamp,
        lc.lockup_end_timestamp,
        GREATEST(
            0,
            TIMESTAMPDIFF(
                nanosecond,
                block_timestamp,
                lockup_end_timestamp
            )
        ) AS lockup_time_left_ns,
        (
            CASE
                WHEN block_timestamp >= lockup_timestamp THEN (
                    CASE
                        WHEN input_release_duration_ns > 0 THEN (
                            CASE
                                WHEN block_timestamp >= lockup_end_timestamp THEN 0 -- everything is released
                                ELSE lockup_amount * lockup_time_left_ns / input_release_duration_ns
                            END
                        )
                        ELSE 0
                    END
                )
                ELSE lockup_amount -- The entire balance is still locked before the lockup timestamp
            END
        ) AS unreleased_amount,
        -- Vesting logic
        lc.vesting_start_timestamp,
        lc.vesting_cliff_timestamp,
        lc.vesting_end_timestamp,
        lc.terminate_vesting_timestamp,
        lc.termination_unvested_amount,
        GREATEST(
            0,
            TIMESTAMPDIFF(
                nanosecond,
                block_timestamp,
                vesting_end_timestamp
            )
        ) AS vesting_time_left_ns,
        TIMESTAMPDIFF(
            nanosecond,
            vesting_start_timestamp,
            vesting_end_timestamp
        ) AS vesting_total_time_ns,
        lc._inserted_timestamp,
        lc._partition_by_block_number
    FROM
        lockup_contracts__parsed AS lc,
        dates AS d
    WHERE
        d.utc_date BETWEEN lc._lockup_start_date
        AND lc._lockup_end_date
),
lockup_contracts_daily_balance__prep_2 AS (
    SELECT
        lc.*,
        SUM(COALESCE(dtw.withdrawn_amount, 0)) over (
            PARTITION BY lc.lockup_account_id,
            lc.lockup_index
            ORDER BY
                lc.utc_date rows BETWEEN unbounded preceding
                AND CURRENT ROW
        ) AS termination_withdrawn_amount
    FROM
        lockup_contracts_daily_balance__prep_1 AS lc
        LEFT JOIN daily_termination_withdrawn_amount AS dtw
        ON dtw.lockup_account_id = lc.lockup_account_id
        AND dtw.utc_date = lc.utc_date
),
lockup_contracts_daily_balance AS (
    SELECT
        lc.*,
        -- Vesting logic
        -- Not 100% accurate due to private vesting lockups (unknown/hidden vesting parameters)
        (
            CASE
                WHEN block_timestamp >= terminate_vesting_timestamp THEN termination_unvested_amount - termination_withdrawn_amount
                ELSE (
                    CASE
                        WHEN block_timestamp < vesting_cliff_timestamp THEN lockup_amount -- Before the cliff, nothing is vested
                        WHEN block_timestamp >= vesting_end_timestamp THEN 0 -- After the end, everything is vested
                        ELSE lockup_amount * vesting_time_left_ns / vesting_total_time_ns
                    END
                )
            END
        ) AS unvested_amount,
        -- Combined logic
        GREATEST(
            unreleased_amount - termination_withdrawn_amount,
            COALESCE(
                unvested_amount,
                0
            )
        ) AS locked_amount,
        locked_amount - COALESCE(LAG(locked_amount) over (PARTITION BY lc.lockup_account_id, lc.lockup_index
    ORDER BY
        lc.utc_date), 0) AS unlocked_amount_today
    FROM
        lockup_contracts_daily_balance__prep_2 AS lc
)
SELECT
    lockup_account_id,
    lockup_index,
    owner_account_id,
    utc_date,
    block_timestamp,
    lockup_amount,
    deposit_timestamp,
    lockup_timestamp,
    lockup_end_timestamp,
    lockup_time_left_ns,
    unreleased_amount,
    vesting_start_timestamp,
    vesting_cliff_timestamp,
    vesting_end_timestamp,
    terminate_vesting_timestamp,
    termination_unvested_amount,
    vesting_time_left_ns,
    vesting_total_time_ns,
    termination_withdrawn_amount,
    unvested_amount,
    locked_amount,
    unlocked_amount_today,
    _inserted_timestamp,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(['utc_date', 'lockup_account_id', 'lockup_index', 'owner_account_id']) }} AS atlas_daily_lockup_locked_balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    lockup_contracts_daily_balance
