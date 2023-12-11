{{ config(
    materialized = 'view',
    tags = ['core', 'social']
) }}

SELECT
    tx_hash,
    action_id_social,
    block_id,
    block_timestamp,
    signer_id,
    post_type,
    post_text,
    post_image,
    COALESCE(
        social_posts_id,
        {{ dbt_utils.generate_surrogate_key(
            ['action_id_social']
        ) }}
    ) AS fact_posts_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_social__posts') }}
