{{ config(
  materialized = 'incremental',
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE'],
  unique_key = 'action_id',
  incremental_strategy = 'merge',
  tags = ['curated']
) }}

WITH action_events AS(

  SELECT
    tx_hash,
    block_id,
    block_timestamp,
    action_id,
    action_data :deposit :: INT AS deposit,
    predecessor_id,
    receiver_id,
    signer_id,
    receipt_succeeded,
    gas_price,
    gas_burnt,
    tokens_burnt,
    _partition_by_block_number,
    _inserted_timestamp,
    modified_timestamp AS _modified_timestamp
  FROM
    {{ ref('silver__actions_events_s3') }}
  WHERE
    action_name = 'Transfer' 

    {% if var("MANUAL_FIX") %}
      AND {{ partition_load_manual('no_buffer') }}
    {% else %}
            {% if is_incremental() %}
        AND _modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
    {% endif %}
),
txs AS (
  SELECT
    tx_hash,
    tx :receipt ::ARRAY AS tx_receipt,
    block_id,
    block_timestamp,
    tx_receiver,
    tx_signer,
    transaction_fee,
    gas_used,
    tx_succeeded,
    _partition_by_block_number,
    _inserted_timestamp,
    modified_timestamp AS _modified_timestamp
  FROM
    {{ ref('silver__streamline_transactions_final') }}

    {% if var("MANUAL_FIX") %}
      WHERE {{ partition_load_manual('no_buffer') }}
    {% else %}
            {% if is_incremental() %}
        WHERE _modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
    {% endif %}
),
actions AS (
  SELECT
    A.tx_hash,
    A.action_id,
    A.block_id,
    A.block_timestamp,
    t.tx_signer,
    t.tx_receiver,
    A.predecessor_id,
    A.receiver_id,
    A.signer_id,
    A.deposit,
    t.transaction_fee,
    A.gas_burnt AS gas_used,
    A.receipt_succeeded,
    t.tx_succeeded,
    A._partition_by_block_number,
    A._inserted_timestamp,
    A._modified_timestamp
  FROM
    action_events A
    INNER JOIN txs t
    ON A.tx_hash = t.tx_hash
),
FINAL AS (
  SELECT
    block_id,
    block_timestamp,
    action_id,
    deposit,
    tx_hash,
    tx_signer,
    tx_receiver,
    predecessor_id,
    signer_id,
    receiver_id,
    transaction_fee,
    gas_used,
    tx_succeeded,
    receipt_succeeded,
    ARRAY_MIN([tx_succeeded, receipt_succeeded]) :: BOOLEAN AS status,
    _partition_by_block_number,
    _inserted_timestamp,
    _modified_timestamp
  FROM
    actions
)
SELECT
  *,
  {{ dbt_utils.generate_surrogate_key(
    ['action_id']
  ) }} AS transfers_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL
