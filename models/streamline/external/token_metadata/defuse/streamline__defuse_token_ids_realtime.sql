{{ config (
    materialized = "view",
    tags = ['streamline_non_core']
) }}

WITH api_call AS (

    SELECT
        {{ target.database }}.live.udf_api(
            'POST',
            'https://bridge.chaindefuser.com/rpc',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json',
                'fsc-quantum-state',
                'livequery'
            ),
            OBJECT_CONSTRUCT(
                'id',
                'dontcare',
                'jsonrpc',
                '2.0',
                'method',
                'supported_tokens',
                'params',
                []
            )
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
