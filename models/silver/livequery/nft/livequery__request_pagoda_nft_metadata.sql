{{ config(
    materialized = 'incremental',
    unique_key = 'request_id',
    incremental_strategy = 'delete+insert',
    tags = ['livequery']
) }}

WITH request_nft_ids AS (

    SELECT
        contract_account_id,
        token_id,
        nft_id
    FROM
        {{ target.database }}.livequery.nft_metadata_needed
    LIMIT 7
),
-- below CTE for manually coding an nft for response testing
{# manual_testing AS (
SELECT
    'collectables.stlb.near' AS contract_account_id,
    '25161563-5e9a-411a-b13e-62d7f6b9a091_80' AS token_id,
    MD5(
        'collectables.stlb.near' || '25161563-5e9a-411a-b13e-62d7f6b9a091_80'
    ) AS nft_id
),
#}
lq_request AS (
    SELECT
        contract_account_id,
        token_id,
        nft_id,
        'https://near-mainnet.api.pagoda.co/eapi/v1/NFT/' || contract_account_id || '/' || token_id AS res_url,
        ethereum.streamline.udf_api(
            'GET',
            res_url,
            { 
                'x-api-key': '{{ var('PAGODA_API_KEY', Null )}}',
                'Content-Type': 'application/json'
            },
            {}
        ) AS lq_response,
        SYSDATE() AS _inserted_timestamp
    FROM
        request_nft_ids
),
FINAL AS (
    SELECT
        contract_account_id,
        token_id,
        nft_id,
        res_url,
        lq_response,
        lq_response :data :message IS NULL AS call_succeeded,
        MD5(
            nft_id || call_succeeded
        ) AS request_id,
        _inserted_timestamp
    FROM
        lq_request
)
SELECT
    *
FROM
    FINAL
