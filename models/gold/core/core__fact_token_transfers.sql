{{ config(
    materialized = 'incremental',
    unique_key = 'fact_token_transfers_id',
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,contract_address,from_address,to_address,fact_token_transfers_id);",    
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% if is_incremental() and not var("MANUAL_FIX") %}
        {% set max_mod_query %}
        SELECT MAX(modified_timestamp) modified_timestamp
        FROM {{ this }}
        {% endset %}

        {% set max_mod = run_query(max_mod_query)[0][0] %}
        {% if not max_mod or max_mod == 'None' %}
            {% set max_mod = '2099-01-01' %}
        {% endif %}

        {% do log("max_mod: " ~ max_mod, info=True) %}

        {% set min_block_date_query %}
        SELECT MIN(block_timestamp::DATE)
        FROM (
            SELECT MIN(block_timestamp) block_timestamp FROM {{ ref('silver__token_transfer_native') }} WHERE modified_timestamp >= '{{max_mod}}'
            UNION ALL
            SELECT MIN(block_timestamp) block_timestamp FROM {{ ref('silver__token_transfer_deposit') }} WHERE modified_timestamp >= '{{max_mod}}'
            UNION ALL
            SELECT MIN(block_timestamp) block_timestamp FROM {{ ref('silver__token_transfer_ft_transfers_method') }} WHERE modified_timestamp >= '{{max_mod}}'
            UNION ALL
            SELECT MIN(block_timestamp) block_timestamp FROM {{ ref('silver__token_transfer_ft_transfers_event') }} WHERE modified_timestamp >= '{{max_mod}}'
            UNION ALL
            SELECT MIN(block_timestamp) block_timestamp FROM {{ ref('silver__token_transfer_mints') }} WHERE modified_timestamp >= '{{max_mod}}'
            UNION ALL
            SELECT MIN(block_timestamp) block_timestamp FROM {{ ref('silver__token_transfer_orders') }} WHERE modified_timestamp >= '{{max_mod}}'
            UNION ALL
            SELECT MIN(block_timestamp) block_timestamp FROM {{ ref('silver__token_transfer_liquidity') }} WHERE modified_timestamp >= '{{max_mod}}'
            UNION ALL
            SELECT MIN(block_timestamp) block_timestamp FROM {{ ref('silver__token_transfer_wrapped_near') }} WHERE modified_timestamp >= '{{max_mod}}'
        )
        {% endset %}

        {% set min_bd = run_query(min_block_date_query)[0][0] %}
        {% if not min_bd or min_bd == 'None' %}
            {% set min_bd = '2099-01-01' %}
        {% endif %}

        {% do log("min_bd: " ~ min_bd, info=True) %}
    {% endif %}
{% endif %}

WITH native_transfers AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        action_index AS rn,
        'wrap.near' AS contract_address,
        predecessor_id AS from_address,
        receiver_id AS to_address,
        NULL AS memo,
        amount_unadj AS amount_unadj,
        'native' AS transfer_type,
        'Transfer' AS transfer_action,
        receipt_succeeded,
        _partition_by_block_number,
        modified_timestamp
    FROM
        {{ ref('silver__token_transfer_native') }}
        {% if var("MANUAL_FIX") %}
            WHERE {{ partition_load_manual('no_buffer') }}
        {% elif is_incremental() %}
            WHERE block_timestamp::DATE >= GREATEST('{{min_bd}}', SYSDATE() :: DATE - interval '1 day')
        {% endif %}
),
native_deposits AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        action_index AS rn,
        'wrap.near' AS contract_address,
        predecessor_id AS from_address,
        receiver_id AS to_address,
        NULL AS memo,
        amount_unadj AS amount_unadj,
        'native' AS transfer_type,
        'Deposit' AS transfer_action,
        receipt_succeeded,
        _partition_by_block_number,
        modified_timestamp
    FROM
        {{ ref('silver__token_transfer_deposit') }}
        {% if var("MANUAL_FIX") %}
            WHERE {{ partition_load_manual('no_buffer') }}
        {% elif is_incremental() %}
            WHERE block_timestamp::DATE >= GREATEST('{{min_bd}}', SYSDATE() :: DATE - interval '1 day')
        {% endif %}
),
ft_transfers_method AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        rn,
        contract_address,
        from_address,
        to_address,
        memo,
        amount_unadj AS amount_unadj,
        'nep141' AS transfer_type,
        method_name AS transfer_action,
        receipt_succeeded,
        _partition_by_block_number,
        modified_timestamp
    FROM
        {{ ref('silver__token_transfer_ft_transfers_method') }}
        {% if var("MANUAL_FIX") %}
            WHERE {{ partition_load_manual('no_buffer') }}
        {% elif is_incremental() %}
            WHERE block_timestamp::DATE >= GREATEST('{{min_bd}}', SYSDATE() :: DATE - interval '1 day')
        {% endif %}
),
ft_transfers_event AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        rn,
        contract_address,
        from_address,
        to_address,
        memo,
        amount_unadj AS amount_unadj,
        'nep141' AS transfer_type,
        'ft_transfer' AS transfer_action,
        receipt_succeeded,
        _partition_by_block_number,
        modified_timestamp
    FROM
        {{ ref('silver__token_transfer_ft_transfers_event') }}
        {% if var("MANUAL_FIX") %}
            WHERE {{ partition_load_manual('no_buffer') }}
        {% elif is_incremental() %}
            WHERE block_timestamp::DATE >= GREATEST('{{min_bd}}', SYSDATE() :: DATE - interval '1 day')
        {% endif %}
),
mints AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        rn,
        contract_address,
        from_address,
        to_address,
        memo,
        amount_unadj AS amount_unadj,
        'nep141' AS transfer_type,
        'ft_mint' AS transfer_action,
        receipt_succeeded,
        _partition_by_block_number,
        modified_timestamp
    FROM
        {{ ref('silver__token_transfer_mints') }}
        {% if var("MANUAL_FIX") %}
            WHERE {{ partition_load_manual('no_buffer') }}
        {% elif is_incremental() %}
            WHERE block_timestamp::DATE >= GREATEST('{{min_bd}}', SYSDATE() :: DATE - interval '1 day')
        {% endif %}
),
orders AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        rn,
        contract_address,
        from_address,
        to_address,
        memo,
        amount_unadj :: STRING AS amount_unadj,
        'nep141' AS transfer_type,
        'order_added' AS transfer_action,
        receipt_succeeded,
        _partition_by_block_number,
        modified_timestamp
    FROM
        {{ ref('silver__token_transfer_orders') }}
        {% if var("MANUAL_FIX") %}
            WHERE {{ partition_load_manual('no_buffer') }}
        {% elif is_incremental() %}
            WHERE block_timestamp::DATE >= GREATEST('{{min_bd}}', SYSDATE() :: DATE - interval '1 day')
        {% endif %}
),
liquidity AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        rn,
        contract_address,
        from_address,
        to_address,
        memo,
        amount_unadj AS amount_unadj,
        'nep141' AS transfer_type,
        'add_liquidity' AS transfer_action,
        receipt_succeeded,
        _partition_by_block_number,
        modified_timestamp
    FROM
        {{ ref('silver__token_transfer_liquidity') }}
        {% if var("MANUAL_FIX") %}
            WHERE {{ partition_load_manual('no_buffer') }}
        {% elif is_incremental() %}
            WHERE block_timestamp::DATE >= GREATEST('{{min_bd}}', SYSDATE() :: DATE - interval '1 day')
        {% endif %}
),
wrapped_near AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        rn,
        contract_address,
        from_address,
        to_address,
        memo,
        amount_unadj AS amount_unadj,
        'nep141' AS transfer_type,
        method_name AS transfer_action,
        receipt_succeeded,
        _partition_by_block_number,
        modified_timestamp
    FROM
        {{ ref('silver__token_transfer_wrapped_near') }}
        {% if var("MANUAL_FIX") %}
            WHERE {{ partition_load_manual('no_buffer') }}
        {% elif is_incremental() %}
            WHERE block_timestamp::DATE >= GREATEST('{{min_bd}}', SYSDATE() :: DATE - interval '1 day')
        {% endif %}
),
all_transfers AS (
    SELECT * FROM native_transfers
    UNION ALL
    SELECT * FROM native_deposits
    UNION ALL
    SELECT * FROM ft_transfers_method
    UNION ALL
    SELECT * FROM ft_transfers_event
    UNION ALL
    SELECT * FROM mints
    UNION ALL
    SELECT * FROM orders
    UNION ALL
    SELECT * FROM liquidity
    UNION ALL
    SELECT * FROM wrapped_near
),
final_transfers AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        rn,
        contract_address,
        from_address,
        to_address,
        memo,
        amount_unadj,
        transfer_type,
        transfer_action,
        receipt_succeeded,
        {{ dbt_utils.generate_surrogate_key(
            ['receipt_id', 'contract_address', 'amount_unadj', 'from_address', 'to_address', 'rn']
        ) }} AS fact_token_transfers_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp
    FROM
        all_transfers
    {% if is_incremental() and not var("MANUAL_FIX") %}
    WHERE modified_timestamp >= '{{max_mod}}'
    {% endif %}
)
SELECT *
FROM final_transfers
QUALIFY(ROW_NUMBER() OVER (PARTITION BY fact_token_transfers_id ORDER BY modified_timestamp DESC)) = 1
