{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_modified_timestamp::Date'],
    unique_key = 'transfers_native_id',
    incremental_strategy = 'merge',
    tags = ['curated','scheduled_non_core']
) }}

WITH native_transfers AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        predecessor_id AS from_address,
        receiver_id AS to_address,
        IFF(REGEXP_LIKE(deposit, '^[0-9]+$'), deposit, NULL) AS amount_unadjusted,
        --numeric validation (there are some exceptions that needs to be ignored)
        receipt_succeeded,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__transfers_s3') }}
    WHERE
        status = TRUE
        AND deposit != 0 
    {% if var("MANUAL_FIX") %}
            AND {{ partition_load_manual('no_buffer') }}            
    {% elif is_incremental() %}
    AND _modified_timestamp >= (
        SELECT
            MAX(_modified_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
),
FINAL AS (
SELECT 
    block_id,
    block_timestamp,
    tx_hash,
    action_id,
    'wrap.near' AS contract_address,
    from_address :: STRING AS from_address,
    to_address :: STRING AS to_address,
    NULL AS memo,
    '0' AS rn,
    'native' AS transfer_type,
    amount_unadjusted :: STRING AS amount_raw,
    amount_unadjusted :: FLOAT AS amount_raw_precise,
    _inserted_timestamp,
    _modified_timestamp,
    _partition_by_block_number
FROM 
    native_transfers
)
SELECT
    *,
  {{ dbt_utils.generate_surrogate_key(
    ['action_id']
  ) }} AS transfers_native_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL