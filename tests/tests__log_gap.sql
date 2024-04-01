{{ config(
    severity = "error"
) }}

WITH r_logs AS (

    SELECT
        DISTINCT receipt_object_id,
        block_timestamp,
        block_id
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        ARRAY_SIZE(logs) > 0 {% if var('DBT_FULL_TEST') %}
            AND _inserted_timestamp < SYSDATE() - INTERVAL '1 hour'
        {% else %}
            AND _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days'
            AND SYSDATE() - INTERVAL '1 hour'
        {% endif %}
),
logs AS (
    SELECT
        DISTINCT receipt_object_id,
        block_timestamp,
        block_id
    FROM
        {{ ref('silver__logs_s3') }}

        {% if var('DBT_FULL_TEST') %}
        WHERE
            _inserted_timestamp < SYSDATE() - INTERVAL '1 hour'
        {% else %}
        WHERE
            _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days'
            AND SYSDATE() - INTERVAL '1 hour'
        {% endif %}
) -- logs in receipts that do not make it to logs
SELECT
    *
FROM
    r_logs
EXCEPT
SELECT
    *
FROM
    logs
