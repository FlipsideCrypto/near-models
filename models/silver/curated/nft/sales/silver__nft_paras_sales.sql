{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'nft_paras_sales_id',
    incremental_strategy = 'merge',
    tags = ['curated','scheduled_non_core']
) }}
{# Note - multisource model #}
-- TODO ez_actions refactor

WITH actions_events AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        signer_id,
        receiver_id,
        predecessor_id,
        method_name,
        deposit,
        args,
        logs,
        attached_gas,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        receipt_succeeded = TRUE
        AND logs [0] IS NOT NULL 
        AND receiver_id = 'marketplace.paras.near'
        AND method_name IN (
            'resolve_purchase',
            'resolve_offer'
        )
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
-- TODO: Delete this dependencie including SuccessValue in the new version of actions events.
status_value AS (
    SELECT
        tx_hash,
        status_value,
        TRY_PARSE_JSON(REPLACE(LOGS[0] :: STRING, 'EVENT_JSON:', '')) AS event,
        PARSE_JSON(BASE64_DECODE_STRING(status_value:SuccessValue)) as SuccessValue,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        receipt_actions:predecessor_id = 'marketplace.paras.near'
    AND 
        event:event = 'nft_transfer'

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
raw_logs AS (
    SELECT
        *,
        l.index AS logs_index
    FROM
        actions_events A,
        LATERAL FLATTEN(
            input => A.logs
        ) l
),
paras_nft AS (
    SELECT
        action_id,
        block_id,
        block_timestamp,
        tx_hash,
        attached_gas,
        COALESCE(
            args :market_data :owner_id,
            args :sale :owner_id,
            args :seller_id
        ) :: STRING AS seller_address,
        COALESCE(
            args :buyer_id,
            args :offer_data :buyer_id
        ) :: STRING AS buyer_address,
        receiver_id AS platform_address,
        'Paras' AS platform_name,
        COALESCE(
            args :market_data :nft_contract_id,
            args :sale :nft_contract_id,
            args :offer_data :nft_contract_id
        ) :: STRING AS nft_address,
        COALESCE(
            args :market_data :token_id,
            args :sale :token_id,
            args :token_id
        ) :: STRING AS token_id,
        COALESCE(
            args :price,
            args :offer_data :price,
            args: market_data :price
        ) / 1e24 AS price,
        method_name,
        args AS LOG,
        logs_index,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        raw_logs
),
------------------------------- FINAL   -------------------------------
FINAL AS (
    SELECT
        m.*,
        NULL :: STRING AS affiliate_id,
        NULL  :: STRING AS affiliate_amount,
        COALESCE(SuccessValue:payout, SuccessValue) :: VARIANT AS royalties,
        price * 0.02 :: FLOAT AS platform_fee
    FROM
        paras_nft m
    LEFT JOIN
        status_value r 
    ON
        m.tx_hash = r.tx_hash
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['action_id', 'logs_index']
    ) }} AS nft_paras_sales_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL