{{ config(
    materialized = 'incremental',
    incremental_stratege = 'delete+insert',
    unique_key = 'active_day',
    tags = ['atlas']
) }}

WITH dates AS (

    SELECT
        date_day AS active_day
    FROM
        {{ source(
            'crosschain',
            'dim_dates'
        ) }}
    WHERE
        date_day BETWEEN '2020-07-21'
        AND SYSDATE() :: DATE
),
signer_first_date AS (
    SELECT
        address,
        first_tx_timestamp
    FROM
        {{ ref('silver__atlas_address_first_action') }}
),
txns AS (
    SELECT
        block_timestamp :: DATE AS active_day,
        tx_signer,
        first_tx_timestamp,
        COALESCE(
            _inserted_timestamp,
            _load_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ ref('silver__streamline_transactions_final') }} t
        LEFT JOIN signer_first_date s
        ON t.tx_signer = s.address
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
        COUNT(
            DISTINCT IFF(
                first_tx_timestamp >= d.active_day - INTERVAL '30 Days'
                AND first_tx_timestamp < d.active_day,
                tx_signer,
                NULL
            )
        ) AS new_maas,
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
