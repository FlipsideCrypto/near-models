{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'bridge_multichain_id',
    cluster_by = ['block_timestamp::DATE', '_modified_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,destination_address,source_address);",
    tags = ['curated', 'exclude_from_schedule'],
) }}

WITH functioncall AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        method_name,
        args,
        logs,
        receiver_id,
        signer_id,
        receipt_succeeded,
        _inserted_timestamp,
        _partition_by_block_number,
        modified_timestamp
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        method_name = 'ft_transfer'  -- Both directions utilize ft_transfer
        
        {% if var("MANUAL_FIX") %}
            AND {{ partition_load_manual('no_buffer') }}
        {% else %}
{% if is_incremental() %}

        AND
            modified_timestamp >= (
                    SELECT MAX(_modified_timestamp) FROM {{ this }}
)

{% endif %}

{% endif %}

),
inbound AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receiver_id AS token_address,
        args :amount :: INT AS amount_raw,
        args :memo :: STRING AS memo,
        args :receiver_id :: STRING AS destination_address,
        NULL AS source_address,
        '1001313161554' AS destination_chain_id,
        SPLIT(
            memo,
            ':'
        ) [2] :: STRING AS source_chain_id,
        receipt_succeeded,
        method_name,
        'inbound' AS direction,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        functioncall
    WHERE
        signer_id = 'mpc-multichain.near'
),
outbound AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receiver_id AS token_address,
        args :amount :: INT AS amount_raw,
        args :memo :: STRING AS memo,
        SPLIT(
            memo,
            ' '
        ) [0] :: STRING AS destination_address,
        signer_id AS source_address,
        SPLIT(
            memo,
            ' '
        ) [1] :: STRING AS destination_chain_id,
        '1001313161554' AS source_chain_id,
        receipt_succeeded,
        method_name,
        'outbound' AS direction,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        functioncall
    WHERE
        args :receiver_id :: STRING = 'mpc-multichain.near'
),
FINAL AS (
    SELECT
        *
    FROM
        inbound
    UNION ALL
    SELECT
        *
    FROM
        outbound
)
SELECT
    *,
    amount_raw AS amount_adj,
    'mpc-multichain.near' AS bridge_address,
    'multichain' AS platform,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS bridge_multichain_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
