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
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_social__decoded_actions') }}
