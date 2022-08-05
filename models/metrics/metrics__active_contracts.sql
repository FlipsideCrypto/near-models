{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  tags = ['metrics'],
  cluster_by = ['date']
) }}

WITH txs AS (

  SELECT
    t.block_timestamp :: DATE AS DATE,
    t.tx_receiver
  FROM
    {{ ref('silver__transactions') }}
    t
    JOIN {{ ref('silver__actions_events_function_call') }}
    aefc
    ON t.tx_hash = aefc.tx_hash
  WHERE
    1 = 1

{% if is_incremental() %}
AND {{ incremental_last_x_days(
  "t._inserted_timestamp",
  2
) }}
{% endif %}
),
daily AS (
  SELECT
    txs.date,
    COUNT(
      DISTINCT tx_receiver
    ) AS daily_active_contracts
  FROM
    txs
  GROUP BY
    1
),
weekly AS (
  SELECT
    DATE_TRUNC(
      'week',
      txs.date
    ) :: DATE AS week,
    COUNT(
      DISTINCT tx_receiver
    ) AS weekly_active_contracts
  FROM
    txs
  GROUP BY
    1
),
monthly AS (
  SELECT
    DATE_TRUNC(
      'month',
      txs.date
    ) :: DATE AS MONTH,
    COUNT(
      DISTINCT tx_receiver
    ) AS montlhy_active_contracts
  FROM
    txs
  GROUP BY
    1
)
SELECT
  daily.*,
  weekly_active_contracts,
  montlhy_active_contracts
FROM
  daily
  LEFT JOIN weekly
  ON weekly.week = DATE_TRUNC(
    'week',
    daily.date
  ) :: DATE
  LEFT JOIN monthly
  ON monthly.month = DATE_TRUNC(
    'month',
    daily.date
  ) :: DATE
ORDER BY
  1
