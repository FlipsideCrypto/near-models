{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    tags = ['metrics'],
    cluster_by = ['date']
) }}

WITH txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__action_events') }} ae
    WHERE
       ae.action_name = 'DeployContract'
      {% if is_incremental() %}
      AND
        {{ incremental_last_x_days(
            "_inserted_timestamp",
            2
        ) }}
        {% endif %}
),
active_contracts AS (
    SELECT
        DATE_TRUNC(
            'day',
            block_timestamp
        ) AS DATE,
        COUNT(
            DISTINCT ae.action_data
        ) AS daily_active_contracts,
        SUM(daily_active_contracts) over (
            ORDER BY
                DATE rows BETWEEN 6 preceding
                AND CURRENT ROW
        ) AS rolling_7day_active_contracts,
        SUM(daily_active_contracts) over (
            ORDER BY
                DATE rows BETWEEN 29 preceding
                AND CURRENT ROW
        ) AS rolling_30day_active_contracts
    FROM
        txs
    GROUP BY
        1
)
SELECT
    *
FROM
    active_contracts
