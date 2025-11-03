{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core']
) }}

WITH labels AS (

    SELECT
        address,
        label_subtype,
        project_name AS exchange_name
    FROM
        {{ ref('core__dim_address_labels') }}
    WHERE
        label_type = 'cex'
)
SELECT
    A.*,
    COALESCE(
        from_label.exchange_name,
        to_label.exchange_name
    ) AS exchange_name,
    CASE
        WHEN from_label.address IS NOT NULL
        AND to_label.address IS NULL THEN 'withdrawal'
        WHEN to_label.address IS NOT NULL
        AND from_label.address IS NULL THEN 'deposit'
        WHEN from_label.address IS NOT NULL
        AND to_label.address IS NOT NULL THEN 'internal_transfer'
    END AS direction
FROM
    {{ ref('core__ez_token_transfers') }} A
    LEFT JOIN labels from_label
    ON A.from_address = from_label.address
    LEFT JOIN labels to_label
    ON A.to_address = to_label.address
WHERE
    COALESCE(
        to_label.address,
        from_label.address
    ) IS NOT NULL
