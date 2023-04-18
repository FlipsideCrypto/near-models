with latest as (
  SELECT 
    DISTINCT t.tx_hash
      , t.block_timestamp as dtime
      , method_name
    , t.tx_status
    , logs
    , outcome
    , coalesce(logs[2], logs[1]) as line
      , TO_NUMBER(regexp_substr(line, 'Contract total staked balance is (\\d+)', 1, 1, 'e', 1))/1e24 as balance
      , t.tx_signer
      , r.receiver_id
      , rank() OVER (PARTITION by r.receiver_id ORDER BY dtime DESC) as rank -- gets latest regardless of day

  FROM near.core.fact_actions_events_function_call a
  JOIN near.core.fact_transactions t ON a.tx_hash = t.tx_hash
  JOIN near.core.fact_receipts r ON a.tx_hash = r.tx_hash

  WHERE 1=1
      AND method_name IN('deposit_and_stake','unstake_all', 'stake', 'unstake')
      AND coalesce(logs[2], logs[1]) LIKE ('Contract total staked%')
    AND t.tx_status = 'Success'

  qualify rank=1
),

refine as (
  SELECT 
      SUM(balance) as stake
    , COUNT(DISTINCT receiver_id) as stake_pools
  from latest
  -- WHERE balance>20378 --current min. stake requirement? https://near-staking.com/
)

SELECT * from refine

limit 100