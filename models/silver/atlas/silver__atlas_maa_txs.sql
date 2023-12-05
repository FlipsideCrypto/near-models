{{ config(
    materialized = 'incremental',
    incremental_stratege = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'day',
    tags = ['atlas']
) }}

WITH dates AS (

    SELECT
        date_day AS DAY
    FROM
        {{ source(
            'crosschain',
            'dim_dates'
        ) }}

{% if is_incremental() %}
WHERE
    date_day > (
        SELECT
            MAX(DAY)
        FROM
            {{ this }}
    )
    AND date_day < SYSDATE() :: DATE
{% else %}
WHERE
    date_day BETWEEN '2020-07-22'
    AND SYSDATE() :: DATE
{% endif %}
),
txns AS (
    SELECT
        block_timestamp :: DATE AS active_day,
        tx_signer,
        tx_hash
    FROM
        {{ ref('silver__streamline_transactions_final') }}

{% if is_incremental() %}
WHERE
    block_timestamp :: DATE >= (
        SELECT
            MAX(DAY)
        FROM
            {{ this }}
    ) - INTERVAL '30 days'
{% endif %}
),
maus AS (
    SELECT
        DAY,
        tx_signer,
        COUNT(
            DISTINCT tx_hash
        ) AS txns
    FROM
        dates d
        LEFT OUTER JOIN txns t
        ON t.active_day < d.day
        AND t.active_day >= d.day - INTERVAL '30 day'
    WHERE
        DAY != CURRENT_DATE()
        AND DAY >= '2023-01-01'
    GROUP BY
        1,
        2
),
FINAL AS (
    SELECT
        DAY,
        COUNT(
            DISTINCT CASE
                WHEN txns = 1 THEN tx_signer
            END
        ) AS tx_1,
        COUNT(
            DISTINCT CASE
                WHEN txns = 2 THEN tx_signer
            END
        ) AS tx_2,
        COUNT(
            DISTINCT CASE
                WHEN txns > 2
                AND txns <= 5 THEN tx_signer
            END
        ) AS tx_3_5,
        COUNT(
            DISTINCT CASE
                WHEN txns > 5 THEN tx_signer
            END
        ) AS tx_5
    FROM
        maus
    GROUP BY
        1
)
SELECT
    DAY,
    tx_1,
    tx_2,
    tx_3_5,
    tx_5,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    FINAL
