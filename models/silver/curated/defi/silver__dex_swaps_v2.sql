{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'dex_swaps_v2_id',
    tags = ['curated'],
) }}

WITH swap_logs AS (

    SELECT
        *
    FROM
        {{ ref('silver__logs_s3') }}
    WHERE
        receipt_succeeded
        AND clean_log LIKE 'Swapped%'
),
receipts AS (
    SELECT
        receipt_object_id,
        receipt_actions,
        receiver_id,
        signer_id
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        receipt_object_id IN (
            SELECT
                receipt_object_id
            FROM
                swap_logs
        )
),
swap_outcome AS (
    SELECT
        tx_hash,
        receipt_object_id,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        ROW_NUMBER() over (
            PARTITION BY receipt_object_id
            ORDER BY
                log_index ASC
        ) - 1 AS swap_index,
        clean_log AS LOG,
        REGEXP_REPLACE(
            LOG,
            '.*Swapped (\\d+) (.*) for (\\d+) (.*)',
            '\\1'
        ) :: INT AS amount_out_raw,
        REGEXP_REPLACE(
            LOG,
            '.*Swapped \\d+ (\\S+) for (\\d+) (.*)',
            '\\1'
        ) :: STRING AS token_out,
        REGEXP_REPLACE(
            LOG,
            '.*Swapped \\d+ \\S+ for (\\d+) (.*)',
            '\\1'
        ) :: INT AS amount_in_raw,
        REGEXP_REPLACE(
            LOG,
            '.*Swapped \\d+ \\S+ for \\d+ (.*)',
            '\\1'
        ) :: STRING AS token_in,
        _partition_by_block_number,
        COALESCE(
            _inserted_timestamp,
            _load_timestamp
        ) AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['receipt_object_id', 'log_index']
        ) }} AS dex_swaps_v2_id,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
    FROM
        swap_logs
),
FINAL AS (
    SELECT
        tx_hash,
        o.receipt_object_id,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        swap_index,
        LOG,
        amount_out_raw,
        token_out,
        amount_in_raw,
        token_in,
        ARRAY_SIZE(
            receipt_actions :receipt :Action :actions
        ) AS action_ct,
        receipt_actions :receipt :Action :actions :: variant AS actions, -- TODO drop col
        TRY_PARSE_JSON(
            TRY_BASE64_DECODE_STRING(
                IFF(
                    receipt_actions :receipt :Action :actions [1] :FunctionCall :method_name = 'swap', -- early Ref and some Ref v2 use a deposit or auth fn call in index 0
                    receipt_actions :receipt :Action :actions [1] :FunctionCall :args,
                    receipt_actions :receipt :Action :actions [0] :FunctionCall :args
                )
            )
        ) AS decoded_action,
        TRY_PARSE_JSON(
            COALESCE(
                COALESCE(
                    decoded_action :msg,
                    decoded_action :operation: Swap, -- Swap must be capitalized! Autoformat may change to "swap"
                    decoded_action
                ) :actions [swap_index],
                decoded_action :msg
            )
        ) AS swap_input_data,
        r.receiver_id AS receipt_receiver_id,
        r.signer_id AS receipt_signer_id,
        _partition_by_block_number,
        _inserted_timestamp,
        dex_swaps_v2_id,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
    FROM
        swap_outcome o
        LEFT JOIN receipts r USING (receipt_object_id)
)
SELECT
    *
FROM
    FINAL
