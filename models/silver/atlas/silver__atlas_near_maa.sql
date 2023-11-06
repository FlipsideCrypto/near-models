{{ config(
    materialized = 'incremental',
    incremental_stratege = 'delete+insert',
    unique_key = 'day',
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
),
txns AS (
    SELECT
        DATE_TRUNC(
            'day',
            block_timestamp
        ) AS active_day,
        tx_signer,
        COALESCE(
            _inserted_timestamp,
            _load_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ ref('silver__streamline_transactions_final') }}
     
    {% if var("MANUAL_FIX") %}
        WHERE {{ partition_load_manual('no_buffer') }}
    {% else %}
        WHERE {{ incremental_load_filter('_inserted_timestamp') }}
    {% endif %}
),
FINAL AS (
    SELECT
        d.active_day,
        COUNT(
            DISTINCT tx_signer
        ) AS maa,
        max(_inserted_timestamp) AS _inserted_timestamp
    FROM
        dates d
        LEFT OUTER JOIN txns t
        ON t.active_day < d.active_day
        AND t.active_day >= d.active_day - INTERVAL '30 days'
    WHERE
        d.active_day != DATE_TRUNC('day', SYSDATE())
    GROUP BY 
        1
)
SELECT
    *
FROM
    FINAL
