{{ config(
    materialized = 'view'
) }}

SELECT
    account_id AS address,
    epoch_block_height :: INT AS epoch_block_id,
    epoch_date :: DATE AS epoch_date,
    liquid,
    lockup_account_id AS lockup_address,
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
    __HEVO__LOADED_AT,
    {{ dbt_utils.generate_surrogate_key(
        ['account_id','epoch_block_height']
    ) }} AS near_balances_daily_id,
    DATEADD(
        ms,
        __HEVO__LOADED_AT,
        '1970-01-01'
    ) AS inserted_timestamp,
    inserted_timestamp AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref(
        'bronze__ft_balances_daily'
    ) }}
