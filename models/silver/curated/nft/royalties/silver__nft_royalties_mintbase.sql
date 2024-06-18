{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['receiver_id'],
    unique_key = 'nft_mintbase_royalty_id',
    incremental_strategy = 'merge',
    tags = ['curated','scheduled_non_core']
) }}


WITH actions_events AS (

    SELECT
        block_id,
        predecessor_id,
        signer_id,
        receiver_id,
        method_name,
        args,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        receipt_succeeded = TRUE
    AND 
        args:royalty_args IS NOT null

    {% if var("MANUAL_FIX") %}
      AND {{ partition_load_manual('no_buffer') }}
    {% else %}
        {% if is_incremental() %}
        AND _modified_timestamp >= (
            SELECT
                MAX(_modified_timestamp)
            FROM
                {{ this }}
        )
        {% endif %}
    {% endif %}
),
FINAL AS (
    SELECT
        block_id,
        predecessor_id
        signer_id,
        receiver_id,
        method_name,
        args:metadata:title as title
        args:metadata:reference AS reference,
        args:metadata:extra AS extra,
        args:owner_id  as owner_id,
        args:royalty_args as royalties_args,
        args:split_owners as split_owners
    FROM
        actions_events
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id', 'reference']
    ) }} AS nft_mintbase_royalty_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL