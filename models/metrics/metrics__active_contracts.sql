{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    tags = ['metrics'],
    cluster_by = ['date']
) }}

WITH txs AS (

    SELECT
      t.block_timestamp::date as date
      , t.tx_receiver
    FROM
        {{ ref('silver__transactions') }} t
    JOIN
        {{ ref('silver__actions_events_function_call') }} aefc
    ON t.tx_hash = aefc.tx_hash
    WHERE
       1=1
      {% if is_incremental() %}
      AND
        {{ incremental_last_x_days(
            "t._inserted_timestamp",
            2
        ) }}
        {% endif %}
      ), daily as (
      SELECT
          txs.date
          , COUNT(distinct tx_receiver) as daily_active_contracts
      FROM txs
      GROUP BY 1
          ),
      weekly as (
          SELECT
              date_trunc('week' , txs.date)::date as week
              , COUNT(distinct tx_receiver) as weekly_active_contracts
      FROM txs
      GROUP BY 1
          ),
      monthly as (
          SELECT
              date_trunc('month' , txs.date)::date as month
              , COUNT(distinct tx_receiver) as montlhy_active_contracts
      FROM txs
      GROUP BY 1
          )
SELECT
   daily.*
  , weekly_active_contracts
  , montlhy_active_contracts
FROM daily
LEFT JOIN weekly
  ON weekly.week   = date_trunc('week'  , daily.date)::date
LEFT JOIN monthly
  ON monthly.month = date_trunc('month' , daily.date)::date
ORDER BY 1
