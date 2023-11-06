{{ config(
    materialized = 'incremental',
    incremental_stratege = 'delete+insert',
    unique_key = 'active_day',
    tags = ['atlas', 'atlas_24h']
) }}

WITH dates AS (

    SELECT
        date_day AS active_day
    FROM
        {{ source(
            'crosschain',
            'dim_dates'
        ) }}
    WHERE date_day between '2020-07-21' AND SYSDATE() :: DATE
),
txns AS (
    SELECT
        block_timestamp :: DATE AS active_day,
        tx_signer,
        COALESCE(
            _inserted_timestamp,
            _load_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ ref('silver__streamline_transactions_final') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_last_x_days(
                '_inserted_timestamp',
                31
            ) }}
        {% endif %}
),
FINAL AS (
    SELECT
        d.active_day,
        COUNT(
            DISTINCT tx_signer
        ) AS maa,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        dates d
        LEFT JOIN txns t
        ON t.active_day < d.active_day
        AND t.active_day >= d.active_day - INTERVAL '30 days'
    WHERE
        d.active_day != SYSDATE() :: DATE
    GROUP BY
        1
)
SELECT
    *
FROM
    FINAL

  