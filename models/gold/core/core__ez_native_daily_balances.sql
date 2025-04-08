{{ config(
    materialized = 'incremental',
    unique_key = "ez_near_daily_balances_id",
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","epoch_date::date"],
    cluster_by = ['epoch_date::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(account_id);",
    tags = ['scheduled_non_core']
) }}

SELECT
    account_id,
    epoch_block_id,
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
    {{ dbt_utils.generate_surrogate_key(
        ['account_id','epoch_block_id']
    ) }} AS ez_near_daily_balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__native_daily_balances') }}

{% if is_incremental() %}
WHERE
    epoch_date >= (
        SELECT
            MAX(epoch_date)
        FROM
            {{ this }}
    )
{% endif %}

--handle potential duplicates introduced by the hevo
qualify(ROW_NUMBER() over (PARTITION BY account_id, epoch_date
ORDER BY
    modified_timestamp DESC) = 1)
