{{ config(
    materialized = "table",
    cluster_by = ["epoch_id"],
    unique_key = "atlas_epochs_id",
    tags = ['scheduled_non_core']
) }}

WITH blocks AS (

    SELECT
        block_id,
        block_timestamp,
        block_author,
        header_json :total_supply :: NUMBER AS total_supply,
        header_json :epoch_id :: STRING AS epoch_id,
        _partition_by_block_number
    FROM
        {{ ref('silver__blocks_final') }}

        {% if var("MANUAL_FIX") %}
            WHERE {{ partition_load_manual('no_buffer') }}
        {% endif %}
),
epochs AS (
    SELECT
        epoch_id,
        MIN(block_id) AS min_block_id,
        MAX(block_id) AS max_block_id,
        COUNT(*) AS blocks,
        COUNT(
            DISTINCT block_author
        ) AS block_producers,
        MIN(block_timestamp) AS start_time,
        MAX(block_timestamp) AS end_time,
        MAX(total_supply) / 1e24 AS total_near_supply,
        ROW_NUMBER() over (
            ORDER BY
                min_block_id ASC
        ) - 1 + 900 AS epoch_num
    FROM
        blocks AS b
    GROUP BY
        1
)
SELECT
    epoch_id,
    min_block_id,
    max_block_id,
    blocks,
    block_producers,
    start_time,
    end_time,
    total_near_supply,
    epoch_num,
    {{ dbt_utils.generate_surrogate_key(['epoch_id']) }} AS atlas_epochs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    epochs
