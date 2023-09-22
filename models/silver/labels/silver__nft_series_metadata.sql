{{ config(
    materialized = 'incremental',
    unique_key = 'metadata_id',
    incremental_strategy = 'delete+insert',
    tags = ['livequery', 'pagoda']
) }}

WITH livequery_response AS (

    SELECT
        *
    FROM
        {{ ref('livequery__request_pagoda_nft_metadata') }}
    WHERE
        call_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
FINAL AS (
    SELECT
        contract_account_id AS contract_address,
        series_id,
        metadata_id,
        lq_response :data :contract_metadata :: variant AS contract_metadata,
        lq_response :data :nft :metadata :: variant AS token_metadata
    FROM
        livequery_response
)
SELECT
    *
FROM
    FINAL
