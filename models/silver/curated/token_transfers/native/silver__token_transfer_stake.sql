{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    unique_key = 'token_transfer_stake_id',
    incremental_strategy = 'merge',
    tags = ['curated','scheduled_non_core']
) }}

WITH action_events AS(

    SELECT
        block_id,
        block_timestamp,
        action_id,
        tx_hash,
        action_data :public_key :: STRING AS public_key,
        action_data :stake :: STRING AS amount_unadj,
        predecessor_id,
        receiver_id,
        signer_id,
        receipt_succeeded,
        gas_price,
        gas_burnt,
        tokens_burnt,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__actions_events_s3') }}
    WHERE
        action_name = 'Stake' {% if var("MANUAL_FIX") %}
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
)
SELECT
    action_id,
    tx_hash,
    block_id,
    block_timestamp,
    public_key,
    amount_unadj,
    amount_unadj :: DOUBLE / pow(
        10,
        24
    ) AS amount_adj,
    predecessor_id,
    receiver_id,
    signer_id,
    receipt_succeeded,
    gas_price,
    gas_burnt,
    tokens_burnt,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['action_id', 'predecessor_id', 'receiver_id', 'amount_unadj']
    ) }} AS token_transfer_stake_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    action_events
