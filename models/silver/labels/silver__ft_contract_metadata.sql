{{ config(
    materialized = 'incremental',
    unique_key = 'contract_address',
    incremental_strategy = 'delete+insert',
    tags = ['livequery', 'nearblocks']
) }}

WITH livequery_results AS (

    SELECT
        *
    FROM
        {{ ref('livequery__request_nearblocks_ft_metadata') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
flatten_results AS (
    SELECT
        page,
        INDEX,
        VALUE :contract :: STRING AS contract_address,
        VALUE :decimals :: INT AS decimals,
        VALUE :icon :: STRING AS icon,
        VALUE :name :: STRING AS NAME,
        VALUE :symbol :: STRING AS symbol,
        VALUE AS DATA,
        _inserted_timestamp,
        _res_id
    FROM
        livequery_results,
        LATERAL FLATTEN(input => TRY_PARSE_JSON(DATA [0] [0]) :data :tokens)
)
SELECT
    *
FROM
    flatten_results
