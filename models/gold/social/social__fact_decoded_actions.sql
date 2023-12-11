{{ config(
    materialized = 'view',
    tags = ['core', 'social']
) }}

SELECT
    action_id_social,
    tx_hash,
    block_id,
    block_timestamp,
    signer_id,
    node,
    node_data,
    COALESCE(
        social_decoded_actions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['action_id_social']
        ) }}
    ) AS fact_decoded_actions_id,
    COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    {{ ref('silver_social__decoded_actions') }}
