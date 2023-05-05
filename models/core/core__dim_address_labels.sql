{{ config(
    materialized = 'view',
    secure = true,
    tags = ['core']
) }}

WITH flipside_labels AS (

    SELECT
        system_created_at,
        blockchain,
        address,
        address_name,
        project_name,
        label_type,
        label_subtype,
        l1_label,
        l2_label,
        creator
    FROM
        {{ ref('silver__address_labels') }}
),
nf_labels AS (
    SELECT
        system_created_at,
        'near' AS blockchain,
        wallet_address AS address,
        NULL AS address_name,
        project_name,
        category AS label_type,
        NULL AS label_subtype,
        NULL AS l1_label,
        NULL AS l2_label,
        creator
    FROM
        {{ ref('silver__foundation_labels') }}
),
FINAL AS (
    SELECT
        *
    FROM
        flipside_labels
    UNION ALL
    SELECT
        *
    FROM
        nf_labels
)
SELECT
    *
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY address
        ORDER BY
            creator
    ) = 1
