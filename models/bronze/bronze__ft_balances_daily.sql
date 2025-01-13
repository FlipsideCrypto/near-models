{{ config(
    materialized = 'view'
) }}

SELECT
    account_id,
    epoch_block_height,
    epoch_date,
    liquid,
    lockup_account_id,
    lockup_liquid,
    lockup_reward,
    lockup_staked,
    lockup_unstaked_not_liquid,
    reward,
    staked,
    storage_usage,
    unstaked_not_liquid,
    __HEVO_ID,
    __HEVO__INGESTED_AT,
    __HEVO__LOADED_AT
FROM
    {{ source(
        'hevo',
        'flipsidecrypto_near_ft_balances_daily'
    ) }}
