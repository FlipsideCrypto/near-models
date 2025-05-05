{% docs core_fact_transactions_live %}
Combines final Gold layer transaction data (`core__fact_transactions`) with specific Bronze layer components (`bronze__FR_transactions`) required for downstream processes.

This model serves as the direct source logic that gets rendered **inside** the `TF_FACT_TRANSACTIONS` User-Defined Table Function (UDTF) via the `livequery_models.get_rendered_model` macro.

**Output Columns:**
- Includes all standard columns from `core__fact_transactions`.
- Adds the following columns from `bronze__FR_transactions` needed for bronze data reconstruction:
    - `data`: The raw VARIANT response from the `EXPERIMENTAL_tx_status` API call.
    - `value`: The structured VARIANT containing transaction metadata used in earlier bronze/silver steps.
    - `partition_key`: The calculated partition key from the bronze layer.

**Purpose & Downstream Use:**
The combined output allows the `SP_REFRESH_FACT_TRANSACTIONS_LIVE` stored procedure (which calls the UDTF generated from this model) to:
1. Populate the `CORE_LIVE.FACT_TRANSACTIONS` hybrid table using the Gold columns.
2. Reconstruct and unload the raw bronze transaction RPC data (using `data`, `value`, `partition_key`, etc.) via a `COPY INTO` to the designated S3 stage, matching the structure of the batch-populated bronze external tables.
{% enddocs %}

{{ config(
    materialized = var('LIVE_TABLE_MATERIALIZATION', 'view'),
    secure = false,
    tags = ['livetable','fact_transactions']
) }}

WITH core_tx AS (
    SELECT * FROM {{ ref('core__fact_transactions') }}
),
bronze_tx AS (
    SELECT * FROM {{ ref('bronze__FR_transactions') }}
)

SELECT
    tx_hash,
    block_id,
    block_timestamp,
    nonce,
    signature,
    tx_receiver,
    tx_signer,
    tx,
    gas_used,
    transaction_fee,
    attached_gas,
    tx_succeeded,
    fact_transactions_id,
    inserted_timestamp,
    modified_timestamp,
    btx.data,
    btx.value,
    btx.partition_key

FROM
    core_tx ctx
JOIN bronze_tx btx
ON ctx.tx_hash = btx.data:transaction:hash
