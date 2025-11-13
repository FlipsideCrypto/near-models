{{ config (
    materialized = "view",
    tags = ['streamline_non_core']
) }}

WITH api_call AS (

    SELECT
        {{ target.database }}.live.udf_api(
            'GET',
            'https://chainlist.org/rpcs.json',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json',
                'fsc-quantum-state',
                'livequery'
            ),
            {}
        ) :: variant AS response
)
SELECT
    response
FROM
    api_call
WHERE
    response IS NOT NULL
    AND response :status_code :: INT IN (
        200,
        201
    )
