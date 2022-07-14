{% test tx_gaps(
    model,
    column_name,
    column_block,
    column_tx_count
) %}
WITH block_base AS (
    SELECT
        {{ column_block }},
        {{ column_tx_count }}
    FROM
        {{ ref('silver__blocks') }}
),
model_name AS (
    SELECT
        {{ column_block }},
        COUNT(
            DISTINCT {{ column_name }}
        ) AS model_tx_count
    FROM
        {{ model }}
    GROUP BY
        {{ column_block }}
)
SELECT
    block_base.{{ column_block }},
    {{ column_tx_count }},
    model_name.{{ column_block }},
    model_tx_count
FROM
    block_base
    LEFT JOIN model_name
    ON block_base.{{ column_block }} = model_name.{{ column_block }}
WHERE
    {{ column_tx_count }} <> model_tx_count {% endtest %}
