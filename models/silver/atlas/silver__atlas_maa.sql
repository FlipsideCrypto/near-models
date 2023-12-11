{{ config(
    materialized = 'incremental',
    incremental_stratege = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'day',
    tags = ['atlas']
) }}

WITH dates AS (

    SELECT
        date_day AS day
    FROM
        {{ source(
            'crosschain',
            'dim_dates'
        ) }}

{% if is_incremental() %}
WHERE
    date_day > (
        SELECT
            MAX(day)
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
signer_first_date AS (
    SELECT
        address,
        first_tx_timestamp
    FROM
        {{ ref('silver__atlas_address_first_action') }}
),
txns AS (
    SELECT
        block_timestamp :: DATE AS day,
        tx_signer,
        first_tx_timestamp,
        COALESCE(
            _inserted_timestamp,
            _load_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ ref('silver__streamline_transactions_final') }}
        t
        LEFT JOIN signer_first_date s
        ON t.tx_signer = s.address

        {% if var("MANUAL_FIX") %}
    WHERE
        {{ partition_load_manual('no_buffer') }}
    {% else %}

{% if is_incremental() %}
WHERE
    block_timestamp :: DATE >= (
        SELECT
            MAX(day)
        FROM
            {{ this }}
    ) - INTERVAL '30 days'
{% endif %}
{% endif %}
),
FINAL AS (
    SELECT
        d.day,
        COUNT(
            DISTINCT tx_signer
        ) AS maa,
        COUNT(
            DISTINCT IFF(
                first_tx_timestamp >= d.day - INTERVAL '30 Days'
                AND first_tx_timestamp < d.day,
                tx_signer,
                NULL
            )
        ) AS new_maas,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        dates d
        LEFT JOIN txns t
        ON t.day < d.day
        AND t.day >= d.day - INTERVAL '30 days'
    WHERE
        d.day != SYSDATE() :: DATE
    GROUP BY
        1
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['day']
    ) }} AS atlas_maa_id,
    day,
    maa,
    new_maas,
    maa - new_maas AS returning_maas,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
