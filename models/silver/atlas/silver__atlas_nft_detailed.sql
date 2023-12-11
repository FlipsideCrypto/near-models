{{ config(
    materialized = 'table',
    unique_key = 'atlas_nft_detailed_id',
    tags = ['atlas']
) }}

WITH nft_data AS (

    SELECT
        *
    FROM
        {{ ref('silver__atlas_nft_transactions') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['DAY', 'receiver_id']
    ) }} AS atlas_nft_detailed_id,
    DAY,
    receiver_id,
    COUNT(
        DISTINCT token_id
    ) AS tokens,
    COUNT(
        CASE
            WHEN method_name = 'nft_transfer' THEN tx_hash
        END
    ) AS all_transfers,
    COUNT(
        DISTINCT owner
    ) AS owners,
    COUNT(*) AS transactions,
    COUNT(
        CASE
            WHEN method_name != 'nft_transfer' THEN tx_hash
        END
    ) AS mints,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    nft_data
GROUP BY
    1,
    2,
    3
ORDER BY
    4 DESC
