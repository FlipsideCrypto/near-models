{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'metadata_id',
    incremental_strategy = 'merge',
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
AND _request_timestamp >= (
    SELECT
        MAX(_request_timestamp)
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
        lq_response :data :nft :metadata :: variant AS token_metadata,
        _inserted_timestamp,
        _request_timestamp
    FROM
        livequery_response
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['metadata_id']
    ) }} AS nft_series_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL 
    qualify ROW_NUMBER() over (
        PARTITION BY metadata_id
        ORDER BY
            _request_timestamp DESC
    ) = 1
