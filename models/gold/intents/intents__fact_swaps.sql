{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'fact_swaps_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['scheduled_non_core', 'intents']
) }}

-- depends on {{ ref('silver__logs_s3') }}
-- depends on {{ ref('core__ez_actions') }}
-- depends on {{ ref('intents__fact_transactions') }}

WITH intents_actions AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        tx_succeeded,
        tx_signer,
        receipt_id,
        receipt_predecessor_id,
        receipt_receiver_id,
        receipt_succeeded,
        action_index,
        action_name,
        action_data,
        action_data:method_name::STRING AS method_name,
        action_data:args::STRING AS args,
        actions_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('core__ez_actions') }}
    WHERE
        receipt_receiver_id = 'intents.near'
        AND receipt_succeeded
        AND tx_succeeded

    {% if var("MANUAL_FIX") %}
        AND {{ partition_load_manual('no_buffer', 'block_timestamp::date') }}
    {% else %}
        {% if is_incremental() %}
            AND modified_timestamp > (
                SELECT MAX(modified_timestamp) FROM {{ this }}
            )
        {% endif %}
    {% endif %}
),

intents_logs AS (
    SELECT
        tx_hash,
        receipt_id,
        block_id,
        block_timestamp,
        receiver_id,
        predecessor_id,
        signer_id,
        log_index,
        clean_log,
        TRY_PARSE_JSON(clean_log) AS log_json,
        TRY_PARSE_JSON(clean_log):event::STRING AS log_event,
        TRY_PARSE_JSON(clean_log):standard::STRING AS log_standard,
        receipt_succeeded,
        inserted_timestamp,
        modified_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__logs_s3') }}
    WHERE
        receiver_id = 'intents.near'
        AND receipt_succeeded
        AND TRY_PARSE_JSON(clean_log):standard::STRING IN ('nep245', 'dip4')

    {% if var("MANUAL_FIX") %}
        AND {{ partition_load_manual('no_buffer', 'block_timestamp::date') }}
    {% else %}
        {% if is_incremental() %}
            AND modified_timestamp > (
                SELECT MAX(modified_timestamp) FROM {{ this }}
            )
        {% endif %}
    {% endif %}
),

token_diff_events AS (
    SELECT
        tx_hash,
        receipt_id,
        block_id,
        block_timestamp,
        log_index,
        log_json:data AS data_array,
        evt.value:intent_hash::STRING AS intent_hash,
        evt.value:account_id::STRING AS account_id,
        evt.value:diff AS token_diff,
        evt.value:referral::STRING AS referral,
        evt.index AS event_index,
        inserted_timestamp,
        modified_timestamp,
        _partition_by_block_number
    FROM
        intents_logs,
        LATERAL FLATTEN(input => log_json:data) AS evt
    WHERE
        log_event = 'token_diff'
        AND log_standard = 'dip4'
),

token_diff_flattened AS (
    SELECT
        tx_hash,
        receipt_id,
        block_id,
        block_timestamp,
        log_index,
        intent_hash,
        account_id,
        referral,
        event_index,
        token_key.key::STRING AS token_id,
        token_key.value::STRING AS token_amount_raw,
        CASE
            WHEN token_key.value::NUMERIC < 0 THEN 'token_in'
            WHEN token_key.value::NUMERIC > 0 THEN 'token_out'
            ELSE 'no_change'
        END AS token_direction,
        ABS(token_key.value::NUMERIC)::STRING AS abs_amount_raw,
        inserted_timestamp,
        modified_timestamp,
        _partition_by_block_number
    FROM
        token_diff_events,
        LATERAL FLATTEN(input => token_diff) AS token_key
    WHERE
        token_key.value::NUMERIC != 0
),

intents_events AS (
    SELECT
        tx_hash,
        receipt_id,
        block_id,
        block_timestamp,
        receiver_id,
        predecessor_id,
        log_event,
        token_id,
        owner_id,
        old_owner_id,
        new_owner_id,
        amount_raw::STRING AS amount_raw,
        token_id AS contract_address,
        log_index,
        log_event_index,
        amount_index,
        fact_transactions_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('intents__fact_transactions') }}
    WHERE
        token_id IS NOT NULL

    {% if var("MANUAL_FIX") %}
        AND {{ partition_load_manual('no_buffer', 'block_timestamp::date') }}
    {% else %}
        {% if is_incremental() %}
            AND modified_timestamp > (
                SELECT MAX(modified_timestamp) FROM {{ this }}
            )
        {% endif %}
    {% endif %}
),

intents_joined AS (
    SELECT
        e.tx_hash,
        e.receipt_id,
        e.block_id,
        e.block_timestamp,
        e.receiver_id,
        e.predecessor_id,
        e.log_event,
        e.token_id,
        e.owner_id,
        e.old_owner_id,
        e.new_owner_id,
        e.amount_raw,
        e.contract_address,
        e.log_index,
        e.log_event_index,
        e.amount_index,
        e.fact_transactions_id,
        e.inserted_timestamp,
        e.modified_timestamp,
        a.tx_signer,
        a.action_name,
        a.method_name,
        a.args,
        a.action_data,
        l.clean_log,
        l.log_json,
        l.log_event AS parsed_log_event,
        l.log_standard,
        l._partition_by_block_number
    FROM intents_events e
    LEFT JOIN intents_actions a ON e.tx_hash = a.tx_hash AND e.receipt_id = a.receipt_id
    LEFT JOIN intents_logs l ON e.tx_hash = l.tx_hash AND e.receipt_id = l.receipt_id
    WHERE e.tx_hash IN (
        SELECT DISTINCT tx_hash
        FROM intents_logs
        WHERE receiver_id = 'intents.near'
          AND receipt_succeeded
          AND TRY_PARSE_JSON(clean_log):event::STRING = 'token_diff'
    )
),

intents_raw AS (
    SELECT
        ij.tx_hash,
        ij.receipt_id,
        ij.block_id,
        ij.block_timestamp,
        ij.receiver_id,
        ij.predecessor_id,
        ij.log_event,
        ij.token_id,
        ij.owner_id,
        ij.old_owner_id,
        ij.new_owner_id,
        ij.amount_raw,
        ij.contract_address,
        ij.log_index,
        ij.log_event_index,
        ij.amount_index,
        ij.fact_transactions_id,
        ij.inserted_timestamp,
        ij.modified_timestamp,
        -- Include joined data from actions and logs
        ij.tx_signer,
        ij.action_name,
        ij.method_name,
        ij.args,
        ij.action_data,
        ij.clean_log,
        ij.log_json,
        ij.parsed_log_event,
        ij.log_standard,
        ij._partition_by_block_number
    FROM intents_joined ij
),

intents_order AS (
    SELECT
        tx_hash,
        receipt_id,
        block_id,
        block_timestamp,
        receiver_id,
        predecessor_id,
        token_id,
        contract_address,
        amount_raw::STRING AS amount_raw,
        owner_id,
        old_owner_id,
        log_event,
        log_index,
        log_event_index,
        amount_index,
        inserted_timestamp,
        modified_timestamp,
        -- Include joined data from actions and logs
        tx_signer,
        action_name,
        method_name,
        args,
        action_data,
        clean_log,
        log_json,
        parsed_log_event,
        log_standard,
        _partition_by_block_number,

        -- identify first and last tokens
        ROW_NUMBER() OVER (
            PARTITION BY tx_hash
            ORDER BY log_index, log_event_index, amount_index
        ) AS token_order_asc,
        ROW_NUMBER() OVER (
            PARTITION BY tx_hash
            ORDER BY log_index DESC, log_event_index DESC, amount_index DESC
        ) AS token_order_desc
    FROM intents_raw
),

intents_swap AS (
    SELECT
        tx_hash,
        -- Swap ordering
        MAX(CASE WHEN token_order_asc = 1 THEN token_id END) AS token_in_id,
        MAX(CASE WHEN token_order_desc = 1 THEN token_id END) AS token_out_id,
        MAX(CASE WHEN token_order_asc = 1 THEN contract_address END) AS token_in_address,
        MAX(CASE WHEN token_order_desc = 1 THEN contract_address END) AS token_out_address,
        -- Total amount
        SUM(CASE WHEN token_order_asc = 1 THEN amount_raw::NUMERIC ELSE 0 END)::STRING AS total_amount_in_raw,
        SUM(CASE WHEN token_order_desc = 1 THEN amount_raw::NUMERIC ELSE 0 END)::STRING AS total_amount_out_raw
    FROM intents_order
    GROUP BY tx_hash
),

token_aggregated AS (
    SELECT
        tdf.tx_hash,
        tdf.receipt_id,
        tdf.intent_hash,
        tdf.account_id,
        tdf.referral,
        tdf.token_id,
        SUM(tdf.token_amount_raw::NUMERIC) AS net_amount,
        MIN(tdf.block_id) AS block_id,
        MIN(tdf.block_timestamp) AS block_timestamp,
        MIN(tdf._partition_by_block_number) AS _partition_by_block_number
    FROM token_diff_flattened tdf
    WHERE tdf.intent_hash IS NOT NULL
    GROUP BY tdf.tx_hash, tdf.receipt_id, tdf.intent_hash, tdf.account_id, tdf.referral, tdf.token_id
),

intents_token_flows AS (
    SELECT
        ta.tx_hash,
        ta.receipt_id,
        ta.intent_hash,
        ta.account_id,
        ta.referral,
        MIN(ta.block_id) AS block_id,
        MIN(ta.block_timestamp) AS block_timestamp,
        MIN(ta.account_id) AS signer_id,
        MIN(ta._partition_by_block_number) AS _partition_by_block_number,
        MAX(il.clean_log) AS log,
        MAX(il.log_standard) AS standard,
        OBJECT_CONSTRUCT(
            'intent_type', 'token_diff',
            'intent_hash', ta.intent_hash,
            'account_id', ta.account_id,
            'referral', COALESCE(ta.referral, ''),
            'standard', COALESCE(MAX(il.log_standard), ''),
            'token_in', MAX(CASE WHEN ta.net_amount > 0 THEN ta.token_id END),
            'token_out', MAX(CASE WHEN ta.net_amount < 0 THEN ta.token_id END),
            'amount_in', MAX(CASE WHEN ta.net_amount > 0 THEN ta.net_amount END),
            'amount_out', MAX(CASE WHEN ta.net_amount < 0 THEN ABS(ta.net_amount) END)
        ) AS swap_input_data,
        MAX(CASE WHEN ta.net_amount > 0 THEN ta.token_id END) AS token_in,
        MAX(CASE WHEN ta.net_amount > 0 THEN ta.net_amount END) AS amount_in_raw,
        MAX(CASE WHEN ta.net_amount < 0 THEN ta.token_id END) AS token_out,
        MAX(CASE WHEN ta.net_amount < 0 THEN ABS(ta.net_amount) END) AS amount_out_raw
    FROM token_aggregated ta
    LEFT JOIN intents_logs il ON ta.tx_hash = il.tx_hash AND ta.receipt_id = il.receipt_id AND il.log_event = 'token_diff'
    GROUP BY ta.tx_hash, ta.receipt_id, ta.intent_hash, ta.account_id, ta.referral
),

intents_swaps_identified AS (
    SELECT
        tx_hash,
        receipt_id,
        block_id,
        block_timestamp,
        signer_id,
        swap_input_data,
        log,
        _partition_by_block_number,
        intent_hash,
        account_id,
        referral,
        token_in,
        amount_in_raw,
        token_out,
        amount_out_raw
    FROM intents_token_flows
    WHERE token_in IS NOT NULL
        AND token_out IS NOT NULL
        AND amount_in_raw > 0
        AND amount_out_raw > 0
),

intents_mapped AS (
    SELECT
        tx_hash,
        receipt_id,
        block_id,
        block_timestamp,
        'intents.near' AS receiver_id,
        signer_id,
        intent_hash,
        account_id,
        referral,
        ROW_NUMBER() OVER (PARTITION BY tx_hash ORDER BY intent_hash, receipt_id) - 1 AS swap_index,
        amount_out_raw::STRING AS amount_out_raw,
        token_out,
        amount_in_raw::STRING AS amount_in_raw,
        token_in,
        swap_input_data,
        log,
        _partition_by_block_number
    FROM intents_swaps_identified
    WHERE token_in IS NOT NULL
        AND token_out IS NOT NULL
        AND amount_in_raw > 0
        AND amount_out_raw > 0
        AND intent_hash IS NOT NULL
),

FINAL AS (
    SELECT
        tx_hash,
        receipt_id,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        intent_hash,
        account_id,
        referral,
        swap_index,
        amount_out_raw,
        token_out,
        amount_in_raw,
        token_in,
        swap_input_data,
        log,
        _partition_by_block_number,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'intent_hash', 'swap_index']
        ) }} AS fact_swaps_id
    FROM
        intents_mapped
)

SELECT
    *,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
