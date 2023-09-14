{{ config(
    materialized = 'incremental',
    unique_key = 'nft_id',
    incremental_strategy = 'delete+insert',
    tags = ['livequery']
) }}
{# TODO - only need to request 1 per collection. Individual token can be disregarded. Thus the nft mints table should be "fixed" as well w collection ID col.
 #}
WITH nfts_minted AS (

    SELECT
        receiver_id AS contract_account_id,
        token_id,
        -- pending work here, need  to isolate just series id and request 1 per series but not everything is just series:token. When split by ':', there are many with 3, 4 and 7 parts.
        -- TODO - isolate the individual series to minimize requests.
        -- 80% are 1 or 2, nearly 20% are 3. A tiny fraction 4 and 7
        {# coalesce(split(token_id, ':')[1],
        split(token_id, ':')[0]) as series_id,
        split(token_id, ':')[1] is null as token_id_is_series_id, #}
        MD5(
            receiver_id || token_id
        ) AS nft_id,
        COALESCE(
            _inserted_timestamp,
            _load_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ ref('silver__standard_nft_mint_s3') }}
),
have_metadata AS (
    SELECT
        contract_account_id,
        token_id,
        nft_id,
        _inserted_timestamp
    FROM
        {{ this }}
),
final_nfts_to_request AS (
    SELECT
        *
    FROM
        nfts_minted
    EXCEPT
    SELECT
        *
    FROM
        have_metadata
),
lq_request AS (
    SELECT
        contract_account_id,
        token_id,
        nft_id,
        'https://near-mainnet.api.pagoda.co/eapi/v1/NFT/' || contract_account_id || '/' || token_id AS res_url,
        livequery_dev.live.udf_api(
            'GET',
            res_url,
            { 
                'x-api-key': '{{ var('PAGODA_API_KEY', Null )}}',
                'Content-Type': 'application/json'
            },
            {}
        ) AS lq_response,
        SYSDATE() AS _request_timestamp,
        _inserted_timestamp
    FROM
        final_nfts_to_request
    LIMIT
        {{ var(
            'SQL_LIMIT', 25
        ) }}
), 
FINAL AS (
    SELECT
        contract_account_id,
        token_id,
        nft_id,
        res_url,
        lq_response,
        lq_response :data :message IS NULL AS call_succeeded,
        _request_timestamp,
        _inserted_timestamp
    FROM
        lq_request
)
SELECT
    *
FROM
    FINAL
