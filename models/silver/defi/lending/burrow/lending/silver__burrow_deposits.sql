{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated']
) }}

WITH
deposits AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        method_name,
        args,
        logs,
        receiver_id,
        receipt_succeeded,
        _inserted_timestamp,
        _partition_by_block_number,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}

{% if is_incremental() %}
WHERE
    {{ incremental_load_filter('_modified_timestamp') }}
{% endif %}
),
FINAL AS (
    SELECT
        *,
        args :sender_id AS sender,
        receiver_id AS contract_address,
        PARSE_JSON(SUBSTRING(logs [0], 12)) AS segmented_data,
        segmented_data :data [0] :account_id AS account_id,
        segmented_data :data [0] :token_id AS token_contract_address,
        segmented_data :data [0] :amount :: NUMBER AS amount,
        segmented_data :event AS actions
    FROM
        deposits
    WHERE
        receiver_id = 'contract.main.burrow.near'
        AND method_name = 'ft_on_transfer'
        AND receipt_succeeded = TRUE
    )
SELECT
    tx_hash,
    block_id AS block_number,
    block_timestamp,
    actions,
    contract_address,
    sender,
    token_contract_address,
    amount,
    _inserted_timestamp,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS actions_events_addkey_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
