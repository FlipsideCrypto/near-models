{{ config(
    materialized = 'incremental',
    unique_key = 'TBD',
    tags = ['scheduled']
) }}

WITH nfts_minted AS (

    SELECT
        receiver_id AS contract_account_id,
        token_id,
        MD5(
            contract_account_id || token_id
        ) AS nft_id
    FROM
        {{ ref('silver__standard_nft_mint_s3') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        WHERE
            {{ incremental_load_filter("_inserted_timestamp") }}
        {% endif %}
    LIMIT 10 -- TODO - FOR TESTING, remove for prod
),
lq_request AS (
    SELECT
        ethereum.streamline.udf_api(
            'GET',
            'https://near-mainnet.api.pagoda.co/eapi/v1/NFT/' || contract_account_id || '/' || token_id,
            {
                'x-api-key': '{{ var('PAGODA_API_KEY', Null )}}',
                'Content-Type': 'application/json'
            },
            {}
        ) AS lq_response,
        SYSDATE() AS _request_timestamp
    FROM
        nfts_minted
)
SELECT
    *
FROM
    lq_request

{# TODO
- ensure deduplication, exclude metadata already requested, route API errors 
- keep original block/inserted timestamps?
- test various batch sizes
 #}