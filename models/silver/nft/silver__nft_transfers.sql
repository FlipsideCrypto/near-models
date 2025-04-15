{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'nft_transfers_id',
    incremental_strategy = 'merge',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,action_id,contract_address,from_address,to_address,token_id);",
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
        logs,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        receipt_succeeded = TRUE
        AND logs [0] IS NOT NULL
        {% if var("MANUAL_FIX") %}
        AND {{ partition_load_manual('no_buffer') }}
        {% else %}
            {% if is_incremental() %}
            AND modified_timestamp >= (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
        {% endif %}
), 

--------------------------------    NFT Transfers    --------------------------------
nft_logs AS (
    SELECT
        block_id,
        signer_id,
        block_timestamp,
        tx_hash,
        action_id,
        TRY_PARSE_JSON(REPLACE(b.value, 'EVENT_JSON:')) AS DATA,
        receiver_id AS contract_id,
        b.index as logs_rn,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        actions_events
        JOIN LATERAL FLATTEN(
            input => logs
        ) b
    WHERE
        DATA :event IN (
            'nft_transfer',
            'nft_mint'
        )
),
--------------------------------        FINAL      --------------------------------
nft_transfers AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        contract_id :: STRING AS contract_address,
        COALESCE(
            A.value :old_owner_id,
            signer_id
        ) :: STRING AS from_address,
        COALESCE(
            A.value :new_owner_id,
            A.value :owner_id
        ) :: STRING AS to_address,
        A.value :token_ids AS token_ids,
        token_ids [0] :: STRING AS token_id,
        logs_rn + A.index as transfer_rn,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        nft_logs
        JOIN LATERAL FLATTEN(
            input => DATA :data
        ) A
    WHERE
        token_id IS NOT NULL
),

nft_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        contract_address,
        from_address,
        to_address,
        B.value :: STRING AS token_id,
        transfer_rn + B.index as rn,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        nft_transfers
    JOIN LATERAL FLATTEN(
            input => token_ids
        ) B
),
FINAL AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        rn,
        contract_address,
        from_address,
        to_address,
        token_id,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        nft_final
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'action_id','contract_address','from_address','to_address','token_id','rn']
    ) }} AS nft_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL