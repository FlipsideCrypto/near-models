{{ config(
    severity = "error",
    tags = ['gap_test']
) }}


WITH 
r_transactions AS (
    SELECT
        DISTINCT tx_hash,
        block_id,
        _signer_id AS signer_id
    FROM
        {{ ref('silver__streamline_transactions') }}

    {% if var('DBT_FULL_TEST') %}
    WHERE
        _inserted_timestamp < SYSDATE() - INTERVAL '1 hour'
    {% else %}
    WHERE
        _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days'
        AND SYSDATE() - INTERVAL '1 hour'
    {% endif %}
),
f_transactions AS (
    SELECT
        DISTINCT tx_hash,
        block_id,
        tx_signer AS signer_id
    FROM
        {{ ref('silver__streamline_transactions_final') }}

    {% if var('DBT_FULL_TEST') %}
    WHERE
        _inserted_timestamp < SYSDATE() - INTERVAL '1 hour'
    {% else %}
    WHERE
        _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days'
        AND SYSDATE() - INTERVAL '1 hour'
    {% endif %}
)
SELECT
    r_transactions.tx_hash AS tx_hash,
    r_transactions.block_id,
    r_transactions.signer_id
FROM
   r_transactions
LEFT JOIN
    f_transactions
ON
    r_transactions.tx_hash = f_transactions.tx_hash
WHERE
    f_transactions.tx_hash IS NULL
