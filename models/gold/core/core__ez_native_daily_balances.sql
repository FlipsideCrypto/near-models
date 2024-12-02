{{ config(
    materialized = 'incremental',
    unique_key = "ez_near_daily_balances_id",
    incremental_strategy = 'merge',
    incremental_predicates = ['DBT_INTERNAL_DEST.epoch_date::DATE >= (select min(epoch_date::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    cluster_by = ['epoch_date::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address);",
    tags = ['scheduled_non_core']
) }}

SELECT
    address,
    epoch_block_id,
    epoch_date,
    liquid,
    lockup_address,
    lockup_liquid,
    lockup_reward,
    lockup_staked,
    lockup_unstaked_not_liquid,
    reward,
    staked,
    storage_usage,
    unstaked_not_liquid,
    {{ dbt_utils.generate_surrogate_key(
        ['address','epoch_block_id']
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
qualify(ROW_NUMBER() over (PARTITION BY address, epoch_date
ORDER BY
    modified_timestamp DESC) = 1)
