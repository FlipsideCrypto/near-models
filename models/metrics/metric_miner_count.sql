{{ config(materialized='table', tags=['metrics']) }}

SELECT 
    date_trunc('day', block_timestamp) as metric_date,
    'daily' as metric_period,
    count(distinct miner) as miner_count
FROM {{ ref("blocks") }}
GROUP BY 1

UNION ALL

SELECT 
    date_trunc('hour', block_timestamp) as metric_date,
    'hourly' as metric_period,
    count(distinct miner) as miner_count
FROM {{ ref("blocks") }}
GROUP BY 1

UNION ALL

SELECT 
    date_trunc('minute', block_timestamp) as metric_date,
    'minute' as metric_period,
    count(distinct miner) as miner_count
FROM {{ ref("blocks") }}
GROUP BY 1
