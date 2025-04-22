{{ config(
    materialized='dynamic_table',
    target_lag='1 minute',
    snowflake_warehouse='DBT_CLOUD', 
    query_tag={
        "model": "near_fact_bocks_live",
        "environment": "{{ target.name }}"
    },
    refresh_mode='incremental',
    transient=false        
) }}


WITH max_gold_block AS (
    
    SELECT
        COALESCE(MAX(block_id), 0) AS max_block_id
    FROM {{ ref('core__fact_blocks') }}
),
chain_head AS (
    SELECT 144753956 AS latest_block_id
),
fetch_parameters AS (
    SELECT
        mgb.max_block_id + 1 AS start_block_id,
        -- TODO: Replace with GREATEST()
        LEAST(ch.latest_block_id - mgb.max_block_id, 10)::INTEGER AS num_rows_to_fetch
    FROM max_gold_block mgb, chain_head ch
)
SELECT *
FROM fetch_parameters fp
