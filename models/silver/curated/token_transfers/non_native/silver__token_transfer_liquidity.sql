{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_modified_timestamp::Date'],
    unique_key = 'transfers_liquidity_id',
    incremental_strategy = 'merge',
    tags = ['curated','scheduled_non_core']
) }}



WITH actions_events AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        signer_id,
        receiver_id,
        action_name,
        method_name,
        deposit,
        logs,
        receipt_succeeded,
        _inserted_timestamp,
        modified_timestamp as _modified_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__token_transfer_base') }}
    {% if var("MANUAL_FIX") %}
            WHERE {{ partition_load_manual('no_buffer') }}            
    {% elif is_incremental() %}
    WHERE _modified_timestamp >= (
        SELECT
            MAX(_modified_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
),
add_liquidity AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        REGEXP_SUBSTR(
            SPLIT.value,
            '"\\d+ ([^"]*)["]',
            1,
            1,
            'e',
            1
        ) :: STRING AS contract_address,
        NULL AS from_address,
        receiver_id AS to_address,
        REGEXP_SUBSTR(
            SPLIT.value,
            '"(\\d+) ',
            1,
            1,
            'e',
            1
        ) :: variant AS amount_unadj,
        'add_liquidity' AS memo,
        INDEX AS rn,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        actions_events,
        LATERAL FLATTEN (
            input => SPLIT(
                REGEXP_SUBSTR(
                    logs [0],
                    '\\["(.*?)"\\]'
                ),
                ','
            )
        ) SPLIT
    WHERE
        logs [0] LIKE 'Liquidity added [%minted % shares'
)
SELECT
    *,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash', 'action_id','contract_address','amount_unadj','from_address','to_address','memo','rn']
  ) }} AS transfers_liquidity_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
    add_liquidity
