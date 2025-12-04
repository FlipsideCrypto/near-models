{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'fact_bridge_activity_id',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,token_address,destination_address,source_address,platform,bridge_address,destination_chain,source_chain,method_name,direction,receipt_succeeded);",
    tags = ['scheduled_non_core'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, BRIDGING' }} }
) }}
-- depends on {{ ref('silver__bridge_rainbow') }}
-- depends on {{ ref('silver__bridge_wormhole') }}
-- depends on {{ ref('silver__bridge_multichain') }}
-- depends on {{ ref('silver__bridge_allbridge') }}
-- depends on {{ ref('silver__bridge_omni') }}
-- depends on {{ ref('intents__fact_bridges') }}

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

WITH rainbow AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw,
        amount_adj,
        destination_address,
        source_address,
        platform,
        bridge_address,
        destination_chain_id AS destination_chain,
        source_chain_id AS source_chain,
        method_name,
        direction,
        receipt_succeeded,
        bridge_rainbow_id AS fact_bridge_activity_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__bridge_rainbow') }}
    {% if var('MANUAL_FIX') %}
        WHERE {{ partition_load_manual('no_buffer', 'floor(block_id, -3)') }}
    {% else %}
        {% if is_incremental() %}
            WHERE modified_timestamp > '{{ max_mod }}'
        {% endif %}
    {% endif %}
),
wormhole AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw,
        amount_adj,
        destination_address,
        source_address,
        platform,
        bridge_address,
        id.blockchain AS destination_chain,
        id2.blockchain AS source_chain,
        method_name,
        direction,
        receipt_succeeded,
        bridge_wormhole_id AS fact_bridge_activity_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__bridge_wormhole') }} b
    LEFT JOIN {{ ref('seeds__wormhole_ids') }} id ON b.destination_chain_id = id.id
    LEFT JOIN {{ ref('seeds__wormhole_ids') }} id2 ON b.source_chain_id = id2.id
    {% if var('MANUAL_FIX') %}
        WHERE {{ partition_load_manual('no_buffer', 'floor(block_id, -3)') }}
    {% else %}
        {% if is_incremental() %}
            WHERE modified_timestamp > '{{ max_mod }}'
        {% endif %}
    {% endif %}
),
multichain AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw,
        amount_adj,
        destination_address,
        source_address,
        platform,
        bridge_address,
        id.blockchain AS destination_chain,
        id2.blockchain AS source_chain,
        method_name,
        direction,
        receipt_succeeded,
        bridge_multichain_id AS fact_bridge_activity_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__bridge_multichain') }} b
    LEFT JOIN {{ ref('seeds__multichain_ids') }} id ON b.destination_chain_id = id.id
    LEFT JOIN {{ ref('seeds__multichain_ids') }} id2 ON b.source_chain_id = id2.id
    {% if var('MANUAL_FIX') %}
        WHERE {{ partition_load_manual('no_buffer', 'floor(block_id, -3)') }}
    {% else %}
        {% if is_incremental() %}
            WHERE modified_timestamp > '{{ max_mod }}'
        {% endif %}
    {% endif %}
),
allbridge AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw,
        amount_adj,
        destination_address,
        source_address,
        platform,
        bridge_address,
        id.blockchain AS destination_chain,
        id2.blockchain AS source_chain,
        method_name,
        direction,
        receipt_succeeded,
        bridge_allbridge_id AS fact_bridge_activity_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__bridge_allbridge') }} b
    LEFT JOIN {{ ref('seeds__allbridge_ids') }} id ON b.destination_chain_id = id.id
    LEFT JOIN {{ ref('seeds__allbridge_ids') }} id2 ON b.source_chain_id = id2.id
    {% if var('MANUAL_FIX') %}
        WHERE {{ partition_load_manual('no_buffer', 'floor(block_id, -3)') }}
    {% else %}
        {% if is_incremental() %}
            WHERE modified_timestamp > '{{ max_mod }}'
        {% endif %}
    {% endif %}
),
omni AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw,
        amount_adj,
        destination_address,
        source_address,
        platform,
        bridge_address,
        destination_chain_id AS destination_chain,
        source_chain_id AS source_chain,
        method_name,
        direction,
        receipt_succeeded,
        bridge_omni_id AS fact_bridge_activity_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__bridge_omni') }}
),
intents AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_unadj AS amount_raw,
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
        fact_bridges_id AS fact_bridge_activity_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('intents__fact_bridges') }}
    {% if var('MANUAL_FIX') %}
        WHERE {{ partition_load_manual('no_buffer', 'floor(block_id, -3)') }}
    {% else %}
        {% if is_incremental() %}
            WHERE modified_timestamp > '{{ max_mod }}'
        {% endif %}
    {% endif %}
),
FINAL AS (
    SELECT
        *
    FROM
        rainbow
    UNION ALL
    SELECT
        *
    FROM
        wormhole
    UNION ALL
    SELECT
        *
    FROM
        multichain
    UNION ALL
    SELECT
        *
    FROM
        allbridge
    UNION ALL
    SELECT
        *
    FROM
        omni
    UNION ALL
    SELECT
        *
    FROM
        intents
)
SELECT
    block_id,
    block_timestamp,
    tx_hash,
    token_address,
    amount_raw AS amount_unadj,
    amount_adj,
    destination_address,
    source_address,
    platform,
    bridge_address,
    LOWER(destination_chain) AS destination_chain,
    LOWER(source_chain) AS source_chain,
    method_name,
    direction,
    receipt_succeeded,
    fact_bridge_activity_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    FINAL
