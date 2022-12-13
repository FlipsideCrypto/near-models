{{ config (
    materialized = 'view'
) }}

SELECT
    metadata$filename as filename,
    data,
    

FROM
    {{ source(
        "streamline_dev",
        "blocks"
    ) }}
