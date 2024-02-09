{{ config(
  materialized = 'incremental',
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE'],
  unique_key = 'action_id',
  incremental_strategy = 'merge',
  tags = ['curated']
) }}
{# TODO - time for a v2 #}
WITH action_events AS(

  SELECT
    tx_hash,
    action_id,
    action_data :deposit :: INT AS deposit,
    receiver_id,
    signer_id,
    receipt_succeeded,
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
      {% if var('IS_MIGRATION') %}
          AND {{ incremental_load_filter('_inserted_timestamp') }}
      {% else %}
          AND {{ incremental_load_filter('_modified_timestamp') }}
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
    WHERE
      {{ partition_load_manual('no_buffer') }}
    {% else %}
      {% if var('IS_MIGRATION') %}
          WHERE {{ incremental_load_filter('_inserted_timestamp') }}
      {% else %}
          WHERE {{ incremental_load_filter('_modified_timestamp') }}
      {% endif %}
    {% endif %}
),
receipts AS (
  SELECT
    tx_hash,
    ARRAY_AGG(
      VALUE :id :: STRING
    ) AS receipt_object_id
  FROM
    txs,
    LATERAL FLATTEN(
      input => tx_receipt
    )
  GROUP BY
    1
),
actions AS (
  SELECT
    t.tx_hash,
    A.action_id,
    t.block_id,
    t.block_timestamp,
    t.tx_signer,
    t.tx_receiver,
    A.receiver_id,
    A.signer_id,
    A.deposit,
    r.receipt_object_id,
    t.transaction_fee,
    t.gas_used,
    A.receipt_succeeded,
    t.tx_succeeded,
    t._partition_by_block_number,
    t._inserted_timestamp,
    t._modified_timestamp
  FROM
    txs AS t
    INNER JOIN receipts AS r
    ON r.tx_hash = t.tx_hash
    INNER JOIN action_events AS A
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
    receipt_object_id,
    signer_id,
    receiver_id,
    transaction_fee,
    gas_used,
    tx_succeeded,
    receipt_succeeded,
    ARRAY_MIN([tx_succeeded, receipt_succeeded]) AS status,
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
