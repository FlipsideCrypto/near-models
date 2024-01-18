{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'ref_swaps_id',
    tags = ['curated'],
) }}

WITH ref_finance AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        receiver_id IN (
            'ref-finance.near',
            'v2.ref-finance.near'
        )
        AND block_timestamp >= CURRENT_DATE - INTERVAL '90 days'
        AND receipt_succeeded
),
flatten_actions AS (
    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_object_id,
        receiver_id,
        signer_id,
        INDEX AS action_index,
        logs,
        -- VALUE,
        VALUE :FunctionCall :method_name :: STRING AS method_name,
        TRY_PARSE_JSON(
            TRY_BASE64_DECODE_STRING(
                VALUE :FunctionCall :args
            )
        ) AS args,
        receipt_succeeded,
        _partition_by_block_number,
        COALESCE(
            _inserted_timestamp,
            _load_timestamp
        ) AS _inserted_timestamp
    FROM
        ref_finance,
        LATERAL FLATTEN (
            receipt_actions :receipt :Action :actions
        )
),
flatten_function_call AS (
    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_object_id,
        receiver_id,
        signer_id,
        action_index,
        logs,
        method_name,
        VALUE,
        VALUE :amount_in :: INT AS amount_in,
        VALUE :min_amount_out :: INT AS min_amount_out,
        VALUE :token_in :: STRING AS token_in,
        VALUE :token_out :: STRING AS token_out,
        VALUE :pool_id :: STRING AS pool_id,
        -- TODO check if always int then change dtype
        INDEX AS swap_index
    FROM
        flatten_actions,
        LATERAL FLATTEN(
            args :actions
        )
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_object_id', 'action_index', 'swap_index']
    ) }} AS ref_swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_function_call
