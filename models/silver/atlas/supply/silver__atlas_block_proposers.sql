{{ config(
    materialized = "incremental",
    cluster_by = ["epoch_num"],
    unique_key = "atlas_block_proposers_id",
    merge_exclude_columns = ["inserted_timestamp"],
    incremental_strategy = "merge",
    tags = ['atlas', 'atlas_supply'],
    enabled = False
) }}
{# Potential future table for staking / governance. DISABLED MODEL, keeping as archive for down the line. #}
WITH epochs AS (

    SELECT
        *
    FROM
        {{ ref('silver__atlas_supply_epochs') }}
),
blocks AS (
    SELECT
        *
    FROM
        {{ ref('silver__streamline_blocks') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {% if is_incremental() %}
                block_id >= (
                    SELECT
                        MAX(min_block_id)
                    FROM
                        epochs
                )
            {% endif %}
        {% endif %}
),
proposals AS (
    SELECT
        b.block_id,
        b.block_timestamp,
        b.epoch_id,
        vp.value ['account_id'] :: STRING AS validator_address,
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
        COUNT(
            DISTINCT b.block_id
        ) over (
            PARTITION BY b.epoch_id,
            b.block_author
        ) AS blocks_produced,
        COALESCE(
            b._inserted_timestamp,
            b._load_timestamp
        ) AS _inserted_timestamp,
        b._partition_by_block_number
    FROM
        blocks AS b
        INNER JOIN epochs AS e
        ON e.epoch_id = b.epoch_id
        qualify ROW_NUMBER() over (
            PARTITION BY b.epoch_id,
            b.block_author
            ORDER BY
                b.block_timestamp DESC
        ) = 1
)
SELECT
    epoch_id,
    epoch_num,
    validator_address,
    blocks_produced,
    _inserted_timestamp,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(['epoch_id', 'validator_address']) }} AS atlas_block_proposers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    block_producers_per_epoch
