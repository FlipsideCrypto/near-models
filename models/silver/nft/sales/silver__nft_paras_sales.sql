{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    unique_key = 'nft_paras_sales_id',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    tags = ['scheduled_non_core']
) }}

WITH actions AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        action_index,
        receipt_signer_id AS signer_id,
        receipt_receiver_id AS receiver_id,
        receipt_predecessor_id AS predecessor_id,
        action_data :method_name :: STRING AS method_name,
        action_data :args :: VARIANT AS args,
        action_data :deposit :: INT AS deposit,
        action_data :gas :: NUMBER AS attached_gas,
        receipt_status_value,
        _partition_by_block_number
    FROM
        {{ ref('core__ez_actions') }}
    WHERE
        receipt_succeeded
        AND action_name = 'FunctionCall'
        AND (receipt_receiver_id = 'marketplace.paras.near' OR receipt_predecessor_id = 'marketplace.paras.near')
        AND method_name IN (
            'resolve_purchase',
            'resolve_offer',
            'nft_transfer_payout'
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
paras_nft_sales AS (
    SELECT
        receipt_id,
        action_index,
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
            args :market_data :price
        ) / 1e24 AS price,
        method_name,
        args AS LOG,
        _partition_by_block_number
    FROM
        actions
    WHERE
        method_name in ('resolve_purchase', 'resolve_offer')
),
FINAL AS (
    SELECT
        m.*,
        NULL :: STRING AS affiliate_id,
        NULL :: STRING AS affiliate_amount,
        l.receipt_status_value :SuccessValue :payout :: VARIANT AS royalties,
        price * 0.02 :: FLOAT AS platform_fee
    FROM
        paras_nft_sales m
    LEFT JOIN
        (select tx_hash, receipt_status_value from actions where method_name = 'nft_transfer_payout') l 
    ON
        m.tx_hash = l.tx_hash
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id', 'action_index']
    ) }} AS nft_paras_sales_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
