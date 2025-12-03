{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['fact_intent_transactions_id'],
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,receipt_id,token_id);",
    tags = ['scheduled_non_core', 'intents']
) }}

{% if execute %}
    {% if is_incremental() and not var("MANUAL_FIX") %}
    {% do log("Incremental and NOT MANUAL_FIX", info=True) %}
    {% set max_mod_query %}

    SELECT
        MAX(modified_timestamp) modified_timestamp
    FROM
        {{ this }}

    {% endset %}

        {%set max_mod = run_query(max_mod_query) [0] [0] %}
        {% if not max_mod or max_mod == 'None' %}
            {% set max_mod = '2099-01-01' %}
        {% endif %}

        {% do log("max_mod: " ~ max_mod, info=True) %}
        {% set min_block_date_query %}

    SELECT
        MIN(
            block_timestamp::DATE
        )
    FROM
        (
            SELECT
                MIN(block_timestamp) block_timestamp
            FROM
                {{ ref('silver__logs_nep245') }}
            WHERE
                modified_timestamp >= '{{max_mod}}'
            UNION ALL
            SELECT
                MIN(block_timestamp) block_timestamp
            FROM
                {{ ref('silver__logs_dip4') }}
            WHERE
                modified_timestamp >= '{{max_mod}}'
        )
    {% endset %}

        {% set min_bd = run_query(min_block_date_query) [0] [0] %}
        {% if not min_bd or min_bd == 'None' %}
            {% set min_bd = '2099-01-01' %}
        {% endif %}

        {% do log("min_bd: " ~ min_bd, info=True) %}
    {% endif %}
{% endif %}

WITH
nep245_events AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        log_index,
        log_event_index,
        owner_id,
        old_owner_id,
        new_owner_id,
        memo,
        amount_index,
        amount_raw,
        token_id,
        gas_burnt,
        receipt_succeeded,
        modified_timestamp
    FROM
        {{ ref('silver__logs_nep245') }}
    WHERE
        1=1

    {% if var("MANUAL_FIX") %}
        AND
            {{ partition_load_manual('no_buffer') }}
    {% else %}
        {% if is_incremental() %}
            AND (
                block_timestamp::DATE >= '{{min_bd}}'
                OR modified_timestamp >= '{{max_mod}}'
            )
        {% endif %}
    {% endif %}
),
dip4_metadata AS (
    SELECT
        tx_hash,
        receipt_id,
        referral,
        raw_log_json:data[0]:fees_collected AS fees_collected_raw,
        dip4_version,
        modified_timestamp
    FROM
        {{ ref('silver__logs_dip4') }}
    WHERE
        log_event = 'token_diff'

    {% if var("MANUAL_FIX") %}
        AND
            {{ partition_load_manual('no_buffer') }}
    {% else %}
        {% if is_incremental() %}
            AND (
                block_timestamp::DATE >= '{{min_bd}}'
                OR modified_timestamp >= '{{max_mod}}'
            )
        {% endif %}
    {% endif %}

    -- Deduplicate on tx_hash/receipt_id to get single referral and fees_collected per receipt
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY tx_hash, receipt_id
        ORDER BY referral IS NOT NULL DESC, array_index
    ) = 1
)
SELECT
    nep.block_timestamp,
    nep.block_id,
    nep.tx_hash,
    nep.receipt_id,
    nep.receiver_id,
    nep.predecessor_id,
    nep.log_event,
    nep.log_index,
    nep.log_event_index,
    nep.owner_id,
    nep.old_owner_id,
    nep.new_owner_id,
    nep.memo,
    nep.amount_index,
    nep.amount_raw,
    nep.token_id,
    dip4_meta.referral,
    dip4_meta.fees_collected_raw,
    dip4_meta.dip4_version,
    nep.gas_burnt,
    nep.receipt_succeeded,
    {{ dbt_utils.generate_surrogate_key(
        ['nep.tx_hash', 'nep.receipt_id', 'nep.log_index', 'nep.log_event_index', 'nep.amount_index']
    ) }} AS fact_intent_transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    nep245_events nep
LEFT JOIN
    dip4_metadata dip4_meta
    ON dip4_meta.tx_hash = nep.tx_hash
    AND dip4_meta.receipt_id = nep.receipt_id
