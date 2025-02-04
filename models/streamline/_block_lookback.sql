{{ config (
    materialized = "ephemeral"
) }}

SELECT
    MIN(block_id) AS block_number
FROM
    {{ ref("core__fact_blocks") }}
WHERE
    block_timestamp >= DATEADD('hour', -72, SYSDATE())
    AND block_timestamp < DATEADD('hour', -71, SYSDATE())
