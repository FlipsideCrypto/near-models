{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_modified_timestamp::Date'],
    unique_key = 'action_id',
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
)
SELECT 
    *
FROM native_transfers