{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core']
) }}

SELECT * FROM {{ ref('core__fact_blocks_logic')}}