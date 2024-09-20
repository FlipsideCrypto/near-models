{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_modified_timestamp::Date'],
    unique_key = 'transfers_orders_id',
    incremental_strategy = 'delete+insert',
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
orders AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        receiver_id,
        TRY_PARSE_JSON(REPLACE(g.value, 'EVENT_JSON:')) AS DATA,
        DATA :event :: STRING AS event,
        g.index AS rn,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        actions_events
        JOIN LATERAL FLATTEN(
            input => logs
        ) g
    WHERE
        DATA :event :: STRING = 'order_added'
),
orders_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        f.value :sell_token :: STRING AS contract_address,
        f.value :owner_id :: STRING AS from_address,
        receiver_id :: STRING AS to_address,
        (
            f.value :original_amount
        ) :: variant AS amount_unadj,
        'order' AS memo,
        f.index AS rn,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        orders
        JOIN LATERAL FLATTEN(
            input => DATA :data
        ) f
    WHERE
        amount_unadj > 0
)
SELECT  
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['action_id']
    ) }} AS transfers_orders_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    orders_final
