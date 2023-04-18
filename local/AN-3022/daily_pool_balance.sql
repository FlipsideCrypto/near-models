

with
all_staking_pools as (
    select
        distinct address
    from near.core.fact_staking_pool_balances
),
dates as (
    select
        date_day as date
    from crosschain.core.dim_dates
    where date_day between '2020-08-25' and CURRENT_DATE
),
boilerplate as (
    select
        ap.address,
        d.date
    from all_staking_pools ap
    cross join dates d
),
daily_balance as (
SELECT
  block_timestamp::date as date,
  address,
  balance
from near.core.fact_staking_pool_balances
qualify row_number() over (
  partition by address, block_timestamp::date 
  order by block_id desc
) = 1
),
imputed_balance as (
select 
    b.date,
    b.address,
    daily.balance,
    -- last_value(daily.balance) over (partition by b.address order by b.date) as imputed_bal_last,
    -- first_value(daily.balance) over (partition by b.address order by b.date) as imputed_bal_first,
    lag(balance) ignore nulls over (partition by address order by date) as imputed_bal_lag,
    coalesce(balance, imputed_bal_lag) as daily_balance
from boilerplate b
left join daily_balance daily using (date, address)
)
-- select sum(daily_balance), count(1) from imputed_balance
-- where date = '2023-04-13'
--     and daily_balance >= 15000;

select
    date,
    address,
    daily_balance as balance
from imputed_balance
where date > current_date - 2
order by date desc, balance desc;

select * from near.core.fact_staking_pool_balances
where block_timestamp::date = '2020-08-28';


-- forked from Daily Staked NEAR Balance @ https://flipsidecrypto.xyz/edit/queries/29da7546-ded6-453b-a9fb-f1293d32d244

 with
  all_staking_pools as (
    select distinct
      address
    from
      near.core.fact_staking_pool_balances
  ),
  dates as (
    select
      date_day as date
    from
      crosschain.core.dim_dates
    where
      date_day between '2020-08-25' and CURRENT_DATE
  ),
  boilerplate as (
    select
      ap.address,
      d.date
    from
      all_staking_pools ap
      cross join dates d
  ),
  daily_balance as (
    SELECT
      block_timestamp::date as date,
      address,
      balance
    from
      near.core.fact_staking_pool_balances
    qualify
      row_number() over (
        partition by
          address,
          block_timestamp::date
        order by
          block_id desc
      ) = 1
  ),
  imputed_balance as (
    select
      b.date,
      b.address,
      daily.balance,
      lag(balance) ignore nulls over (
        partition by
          address
        order by
          date
      ) as imputed_bal_lag,
      coalesce(balance, imputed_bal_lag) as daily_balance
    from
      boilerplate b
      left join daily_balance daily using (date, address)
  )
select
  date,
  address,
  daily_balance as balance
from imputed_balance
order by date asc, balance desc
limit 100;
