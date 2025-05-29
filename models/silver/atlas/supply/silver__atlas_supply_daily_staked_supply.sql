{{ config(
    materialized = "table",
    cluster_by = ["utc_date"],
    unique_key = "atlas_daily_staked_supply_id",
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core', 'atlas'],
    enabled = False
) }}

{# 
DISABLED / archived 12/1/2023
Note - seems like a more complicated way to get to the result in silver__pool_balances(_daily). Both look for logs that emit the updated staked total,
but this query is calculating extra proposer and epoch stats and not using them.
Unnecessary compute, possibly from a separate analytical process.
The numbers do seem to differ, but individual pool balances in my silver table match near-staking exactly.
#}

WITH receipts AS (

    SELECT
        *,
        receipt_actions AS actions,
        execution_outcome AS outcome
    FROM
        {{ ref('silver__streamline_receipts_final') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp :: DATE >= (
        SELECT
            MAX(utc_date) - INTERVAL '2 days'
        FROM
            {{ this }}
    )
{% endif %}
),
function_call AS (
    SELECT
        distinct tx_hash
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        method_name IN (
            'ping',
            'stake',
            'unstake',
            'stake_all',
            'unstake_all',
            'deposit_and_stake'
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(utc_date) - INTERVAL '2 days'
    FROM
        {{ this }}
)
{% endif %}
),
blocks AS (
    SELECT
        *
    FROM
        {{ ref('silver__streamline_blocks') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp :: DATE >= (
        SELECT
            MAX(utc_date) - INTERVAL '2 days'
        FROM
            {{ this }}
    )
{% endif %}
),
epochs AS (
    SELECT
        *
    FROM
        {{ ref('silver__atlas_supply_epochs') }}
),
staking_actions AS (
    SELECT
        r.tx_hash,
        r.block_timestamp,
        r.receiver_id AS validator_address,
        REPLACE(
            SPLIT(
                l.value :: STRING,
                ': Contract received total'
            ) [0],
            'Epoch ',
            ''
        ) :: INTEGER AS epoch_num,
        SPLIT(
            SPLIT(
                l.value :: STRING,
                'New total staked balance is '
            ) [1],
            '. Total number of shares'
        ) [0] :: bigint / 1e24 AS staked_balance
    FROM
        receipts AS r,
        LATERAL FLATTEN(
            input => r.logs
        ) AS l
    WHERE
        (RIGHT(receiver_id, 12) = '.poolv1.near'
        OR RIGHT(receiver_id, 10) = '.pool.near')
        AND r.tx_hash IN (
            SELECT
                tx_hash
            FROM
                function_call
        )
        AND LEFT(
            l.value :: STRING,
            6
        ) = 'Epoch ' qualify ROW_NUMBER() over (
            PARTITION BY epoch_num,
            validator_address
            ORDER BY
                block_timestamp DESC
        ) = 1
)
,
proposals AS (
    SELECT
        b.block_id,
        b.block_timestamp,
        b.epoch_id,
        vp.value ['account_id'] AS validator_address,
        vp.value ['stake'] :: bigint / 1e24 AS staked_balance
    FROM
        blocks AS b,
        LATERAL FLATTEN(
            input => b.chunks
        ) AS C,
        LATERAL FLATTEN(
            input => C.value ['validator_proposals']
        ) AS vp -- WHERE b.block_timestamp >= '2021-09-01'
        qualify ROW_NUMBER() over (
            PARTITION BY validator_address,
            epoch_id
            ORDER BY
                block_timestamp DESC
        ) = 1
),
proposals_per_epoch AS (
    SELECT
        p.block_timestamp,
        p.epoch_id,
        p.validator_address,
        p.staked_balance,
        e.epoch_num
    FROM
        proposals AS p
        INNER JOIN epochs AS e
        ON e.epoch_id = p.epoch_id qualify ROW_NUMBER() over (
            PARTITION BY epoch_num,
            validator_address
            ORDER BY
                block_timestamp DESC
        ) = 1
),
block_producers_per_epoch AS (
    SELECT
        b.epoch_id,
        e.epoch_num,
        b.block_author AS validator_address,
        sa.staked_balance,
        COUNT(
            DISTINCT b.block_id
        ) over (
            PARTITION BY b.epoch_id,
            b.block_author
        ) AS blocks_produced
    FROM
        blocks AS b
        INNER JOIN epochs AS e
        ON e.epoch_id = b.epoch_id
        LEFT JOIN staking_actions AS sa
        ON sa.epoch_num = e.epoch_num
        AND sa.validator_address = b.block_author qualify ROW_NUMBER() over (
            PARTITION BY b.epoch_id,
            b.block_author
            ORDER BY
                b.block_timestamp DESC
        ) = 1
),
dim_validators AS (
    SELECT
        validator_address,
        MIN(start_epoch) AS start_epoch,
        MIN(start_time) AS start_time
    FROM
        (
            SELECT
                validator_address,
                MIN(epoch_num) AS start_epoch,
                MIN(block_timestamp) AS start_time
            FROM
                staking_actions AS sa
            GROUP BY
                1
            UNION ALL
            SELECT
                block_author AS validator_address,
                MIN(
                    e.epoch_num
                ) AS start_epoch,
                MIN(
                    b.block_timestamp
                ) AS start_time
            FROM
                blocks AS b
                LEFT JOIN epochs AS e
                ON b.block_id BETWEEN e.min_block_id
                AND e.max_block_id
            GROUP BY
                1
        ) AS x
    GROUP BY
        1
),
dim_table AS (
    SELECT
        v.validator_address,
        e.epoch_num,
        e.start_time,
        e.total_near_supply
    FROM
        dim_validators AS v,
        epochs AS e
    WHERE
        v.start_epoch <= e.epoch_num
),
validator_status_per_epoch AS (
    SELECT
        dt.epoch_num,
        dt.start_time,
        dt.validator_address,
        COALESCE(
            LAST_VALUE(COALESCE(bp.staked_balance, p.staked_balance)) ignore nulls over (
                PARTITION BY dt.validator_address
                ORDER BY
                    dt.epoch_num rows BETWEEN unbounded preceding
                    AND CURRENT ROW
            ),
            0
        ) AS staked_balance,
        bp.blocks_produced,
        (
            CASE
                WHEN p.validator_address IS NOT NULL THEN TRUE
                ELSE FALSE
            END
        ) AS is_proposer
    FROM
        dim_table AS dt
        LEFT JOIN block_producers_per_epoch AS bp
        ON bp.epoch_num = dt.epoch_num
        AND bp.validator_address = dt.validator_address
        LEFT JOIN proposals_per_epoch AS p
        ON p.epoch_num = dt.epoch_num
        AND p.validator_address = dt.validator_address
),
epoch_stats AS (
    SELECT
        epoch_num,
        start_time,
        SUM(staked_balance) AS total_near_staked
    FROM
        validator_status_per_epoch
    WHERE
        staked_balance > 0
    GROUP BY
        1,
        2
),
epoch_stats_2 AS (
    SELECT
        es.*,
        de.total_near_supply,
        de.total_near_supply - es.total_near_staked AS other_near_supply,
        100.00 * total_near_staked / total_near_supply AS perc_staked_supply
    FROM
        epoch_stats AS es
        LEFT JOIN epochs AS de
        ON de.epoch_num = es.epoch_num
),
FINAL AS (
    SELECT
        start_time :: DATE AS utc_date,
        total_near_staked AS total_staked_supply,
        total_near_supply AS total_supply
    FROM
        epoch_stats_2 qualify ROW_NUMBER() over (
            PARTITION BY utc_date
            ORDER BY
                start_time DESC
        ) = 1
)
SELECT
    utc_date,
    total_staked_supply,
    total_supply,
    {{ dbt_utils.generate_surrogate_key(['utc_date']) }} AS atlas_daily_staked_supply_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
