{{ config(
    materialized = 'incremental',
    unique_key = 'action_id',
    cluster_by = ['_load_timestamp::date', '_partition_by_block_number'],
    tags = ['s3_curated', 'social']
) }}

WITH all_social_receipts AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
        AND (LOWER(signer_id) = 'social.near'
        OR LOWER(receiver_id) = 'social.near')
)
SELECT
    *
FROM
    all_social_receipts
