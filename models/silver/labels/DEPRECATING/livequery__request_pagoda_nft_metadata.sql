{{ config(
    materialized = 'incremental',
    unique_key = 'metadata_id',
    incremental_strategy = 'delete+insert',
    tags = ['streamline_non_core']
) }}
-- DEPRECATED
-- DELETE ALONGSIDE DEFI__DIM_NFT_SERIES_METADATA

WITH nfts_minted AS (

    SELECT
        tx_hash,
        block_id,
        receiver_id AS contract_account_id,
        token_id,
        IFF(
            -- if token id does not include a series id, then set contract to series id, else extract from token id
            -- note: if a nft is deployed as its own contract, we will see this behavior
            -- if launched as part of a marketplace contract, like paras, the series id is included in the token id
            SPLIT(
                token_id,
                ':'
            ) [1] IS NULL,
            contract_account_id,
            SPLIT(
                token_id,
                ':'
            ) [0]
        ) AS series_id,
        MD5(
            receiver_id || series_id
        ) AS metadata_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__standard_nft_mint_s3') }}
    WHERE
        contract_account_id NOT IN (
            'nft-assets.l2e.near' -- broken
        )
),

{% if is_incremental() %}
have_metadata AS (
    SELECT
        DISTINCT metadata_id
    FROM
        {{ this }}
),
{% endif %}

final_nfts_to_request AS (
    SELECT
        *
    FROM
        nfts_minted

{% if is_incremental() %}
WHERE
    metadata_id NOT IN (
        SELECT
            metadata_id
        FROM
            have_metadata
    )
{% endif %}
),
lq_request AS (
    SELECT
        tx_hash,
        block_id,
        contract_account_id,
        token_id,
        series_id,
        metadata_id,
        'https://near-mainnet.api.pagoda.co/eapi/v1/NFT/' || contract_account_id || '/' || token_id AS res_url,
        live.udf_api(
            'GET',
            res_url,
            { 
                'x-api-key': '{{ var('PAGODA_API_KEY', NULL) }}',
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
            'SQL_LIMIT', 250
        ) }}
), FINAL AS (
    SELECT
        tx_hash,
        block_id,
        contract_account_id,
        token_id,
        series_id,
        metadata_id,
        res_url,
        lq_response,
        (
            lq_response :data :message IS NULL
        )
        AND (
            lq_response :error IS NULL
        ) AS call_succeeded,
        COALESCE(
            lq_response :data :code :: INT,
            lq_response :status_code :: INT
        ) AS error_code,
        lq_response :data :message :: STRING AS error_message,
        _request_timestamp,
        _inserted_timestamp
    FROM
        lq_request
)
SELECT
    *
FROM
    FINAL
