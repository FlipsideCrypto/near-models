{{ config(
    materialized = 'view',
    tags = ['livequery']
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
),
have_metadata AS (
    SELECT
        contract_account_id,
        token_id,
        nft_id
    FROM
        {{ ref('livequery__request_pagoda_nft_metadata') }}
),
FINAL AS (
    SELECT
        *
    FROM
        nfts_minted
    EXCEPT
    SELECT
        *
    FROM
        have_metadata
)
SELECT
    *
FROM
    FINAL
