{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'nft_sales_id',
    incremental_strategy = 'delete+insert',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,seller_address,buyer_address,nft_address,token_id);",
    tags = ['scheduled_non_core']
) }}

WITH mintbase_nft_sales AS (
    SELECT
        receipt_id,
        action_index,
        block_id,
        block_timestamp,
        tx_hash,
        attached_gas,
        seller_address,
        buyer_address,
        platform_address,
        platform_name,
        nft_address,
        token_id,
        method_name,
        LOG,
        price,
        log_index AS logs_index,
        affiliate_id,
        affiliate_amount,
        royalties,
        platform_fee,
        _partition_by_block_number
    FROM
        {{ ref('silver__nft_mintbase_sales') }}
        {% if var("MANUAL_FIX") %}
        WHERE {{ partition_load_manual('no_buffer') }}
        {% else %}
            {% if is_incremental() %}
            WHERE modified_timestamp >= (
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
        seller_address,
        buyer_address,
        platform_address,
        platform_name,
        nft_address,
        token_id,
        method_name,
        LOG,
        price,
        0 AS logs_index,
        affiliate_id,
        affiliate_amount,
        royalties,
        platform_fee,
        _partition_by_block_number
    FROM
        {{ ref('silver__nft_paras_sales') }}
    {% if var("MANUAL_FIX") %}
      WHERE {{ partition_load_manual('no_buffer') }}
    {% else %}
        {% if is_incremental() %}
        WHERE modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
        {% endif %}
    {% endif %}
),
other_nft_sales AS (
    SELECT
        receipt_id,
        action_index,
        block_id,
        block_timestamp,
        tx_hash,
        attached_gas,
        seller_address,
        buyer_address,
        platform_address,
        platform_name,
        nft_address,
        token_id,
        method_name,
        LOG,
        price,
        0 AS logs_index,
        NULL AS affiliate_id,
        NULL AS affiliate_amount,
        NULL AS royalties,
        NULL AS platform_fee,
        _partition_by_block_number
    FROM
        {{ ref('silver__nft_other_sales') }}
    {% if var("MANUAL_FIX") %}
      WHERE {{ partition_load_manual('no_buffer') }}
    {% else %}
        {% if is_incremental() %}
        WHERE modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
        {% endif %}
    {% endif %}
),
prices AS (
    --get closing price for the hour
    SELECT
        DATE_TRUNC(
            'HOUR',
            hour
        ) AS block_timestamp_hour,
        price as price_usd
    FROM
        {{ ref('silver__complete_token_prices') }}
    WHERE
        token_address = 'wrap.near' qualify ROW_NUMBER() over (
            PARTITION BY block_timestamp_hour
            ORDER BY
                hour DESC
        ) = 1
),
------------------------------- FINAL   -------------------------------
sales_union AS (
    SELECT
        *  
    FROM
        mintbase_nft_sales
    UNION ALL
    SELECT
        *
    FROM
        paras_nft_sales
    UNION ALL
    SELECT
        *
    FROM
        other_nft_sales
),
FINAL AS (
    SELECT
        receipt_id,
        block_id,
        block_timestamp,
        tx_hash,
        attached_gas AS gas_burned,
        seller_address,
        buyer_address,
        platform_address,
        platform_name,
        nft_address,
        token_id,
        method_name,
        LOG,
        price,
        s.price * p.price_usd AS price_usd,
        logs_index,
        affiliate_id,
        affiliate_amount,
        affiliate_amount * p.price_usd AS affiliate_amount_usd,
        royalties,
        platform_fee AS platform_fee,
        platform_fee * p.price_usd AS platform_fee_usd,
        _partition_by_block_number
    FROM
        sales_union s
        ASOF JOIN prices p
        MATCH_CONDITION (
            DATE_TRUNC('hour', s.block_timestamp) 
            >= p.block_timestamp_hour
        )
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id', 'logs_index']
    ) }} AS nft_sales_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
