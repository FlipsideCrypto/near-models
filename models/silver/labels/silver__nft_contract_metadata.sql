{{ config(
    materialized = 'incremental',
    unique_key = 'contract_address',
    incremental_strategy = 'delete+insert',
    tags = ['livequery', 'nearblocks'],
) }}

WITH livequery_results AS (

    SELECT
        *
    FROM
        {{ ref('livequery__request_nearblocks_nft_metadata') }}

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
        VALUE :base_uri :: STRING AS base_uri,
        VALUE :contract :: STRING AS contract_address,
        VALUE :icon :: STRING AS icon,
        VALUE :name :: STRING AS NAME,
        VALUE :symbol :: STRING AS symbol,
        VALUE :tokens :: INT AS tokens,
        VALUE as data,
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
qualify row_number() over (partition by contract_address order by _inserted_timestamp desc) = 1
