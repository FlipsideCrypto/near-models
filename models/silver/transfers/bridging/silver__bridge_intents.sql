{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['bridge_intents_id'],
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,token_address,destination_address,source_address,bridge_address,destination_chain,source_chain,method_name,direction,receipt_succeeded);",
    tags = ['scheduled_non_core']
) }}
-- depends on {{ ref('defi__fact_intents') }}

{% if execute %}
    {% if is_incremental() %}
        {% set max_mod_query %}
            SELECT
                MAX(modified_timestamp) modified_timestamp
            FROM
                {{ this }}
        {% endset %}
        {% set max_mod = run_query(max_mod_query) [0] [0] %}
        {% if not max_mod or max_mod == 'None' %}
            {% set max_mod = '2099-01-01' %}
        {% endif %}
    {% endif %}
{% endif %}

WITH bridge_intents AS (
    SELECT 
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        log_index,
        owner_id,
        token_id,
        amount_raw,
        memo,
        gas_burnt,
        receipt_succeeded,
        fact_intents_id,
        modified_timestamp
    FROM {{ ref('defi__fact_intents') }}
    WHERE 
        log_event IN ('mt_burn', 'mt_mint')
        AND memo IN ('deposit', 'withdraw') 
        AND RIGHT(token_id, 10) = '.omft.near'

    {% if var('MANUAL_FIX') %}
        AND {{ partition_load_manual('no_buffer', 'floor(block_id, -3)') }}
    {% else %}
        {% if is_incremental() %}
            AND modified_timestamp > '{{ max_mod }}'
        {% endif %}
    {% endif %}
),

bridge_intents_parsed AS (
    SELECT 
        *,
        REGEXP_SUBSTR(token_id, 'nep141:([^-\\.]+)', 1, 1, 'e', 1) AS blockchain,
        CASE 
            WHEN token_id LIKE '%-%' THEN 
                REGEXP_SUBSTR(token_id, 'nep141:[^-]+-(.+)\\.omft\\.near', 1, 1, 'e', 1)
            ELSE SPLIT(token_id, ':')[1]
        END AS contract_address_raw
    FROM bridge_intents
),

bridge_intents_mapped AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        'execute_intents' AS method_name, -- Unnecessary to join actions just to get this
        owner_id,
        contract_address_raw AS token_address,
        token_id AS token_address_raw,
        amount_raw AS amount_unadj,
        amount_raw AS amount_adj,
        owner_id AS destination_address,
        owner_id AS source_address,
        'intents' AS platform,
        receiver_id AS bridge_address,
        CASE 
            WHEN log_event = 'mt_mint' AND memo = 'deposit' THEN LOWER(blockchain)
            ELSE 'near'
        END AS source_chain,
        CASE 
            WHEN log_event = 'mt_burn' AND memo = 'withdraw' THEN LOWER(blockchain)
            ELSE 'near'
        END AS destination_chain,
        CASE 
            WHEN log_event = 'mt_mint' AND memo = 'deposit' THEN 'inbound'
            WHEN log_event = 'mt_burn' AND memo = 'withdraw' THEN 'outbound'
        END AS direction,
        receipt_succeeded,
        memo,
        fact_intents_id,
        modified_timestamp
    FROM bridge_intents_parsed
    WHERE blockchain IS NOT NULL
        AND blockchain != ''
        AND blockchain != 'near'
)

SELECT
    block_id,
    block_timestamp,
    tx_hash,
    token_address,
    token_address_raw,
    amount_unadj,
    amount_adj,
    destination_address,
    source_address,
    platform,
    bridge_address,
    destination_chain,
    source_chain,
    method_name,
    direction,
    receipt_succeeded,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'source_chain', 'destination_address', 'token_address', 'amount_unadj']
    ) }} AS bridge_intents_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM bridge_intents_mapped
ORDER BY block_timestamp DESC