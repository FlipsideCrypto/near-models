{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = 'tx_hash',
  cluster_by = ['block_timestamp::DATE','_modified_timestamp::DATE', '_partition_by_block_number'],
  tags = ['receipt_map','scheduled_core']
) }}

WITH int_txs AS (

  SELECT
    block_id,
    tx_hash,
    shard_id,
    transactions_index,
    chunk_hash,
    outcome_receipts,
    _actions,
    _hash,
    _nonce,
    _outcome,
    _public_key,
    _receiver_id,
    _signature,
    _signer_id,
    _partition_by_block_number,
    _inserted_timestamp,
    modified_timestamp AS _modified_timestamp
  FROM
    {{ ref('silver__streamline_transactions') }}

    {% if var('MANUAL_FIX') %}

        WHERE
            {{ partition_load_manual('no_buffer') }}
            
    {% else %}
      WHERE
        {{ partition_incremental_load(
          6000,
          6000,
          0
        ) }}

    {% endif %}
),
int_receipts AS (
  SELECT
    block_id,
    block_timestamp,
    tx_hash,
    receipt_object_id,
    execution_outcome,
    receipt_succeeded,
    gas_burnt,
    _partition_by_block_number,
    _inserted_timestamp,
    modified_timestamp AS _modified_timestamp
  FROM
    {{ ref('silver__streamline_receipts_final') }}

    {% if var('MANUAL_FIX') %}

        WHERE
            {{ partition_load_manual('end') }}
            
    {% else %}

      WHERE
        {{ partition_incremental_load(
          6000,
          6000,
          0
        ) }}

    {% endif %}

),
int_blocks AS (
  SELECT
    block_id,
    block_hash,
    block_timestamp,
    _partition_by_block_number,
    _inserted_timestamp,
    modified_timestamp AS _modified_timestamp
  FROM
    {{ ref('silver__streamline_blocks') }}
  WHERE
    _partition_by_block_number >= (
      SELECT MIN(_partition_by_block_number) FROM int_txs
    )
),
receipt_array AS (
  SELECT
    tx_hash,
    ARRAY_AGG(execution_outcome) within GROUP (
      ORDER BY
        block_timestamp
    ) AS receipt
  FROM
    int_receipts
  GROUP BY
    1
),
base_transactions AS (
  SELECT
    t.tx_hash,
    t.block_id,
    b.block_hash,
    b.block_timestamp,
    t.shard_id,
    transactions_index,
    t.chunk_hash,
    outcome_receipts,
    OBJECT_CONSTRUCT(
      'actions',
      _actions,
      'hash',
      _hash,
      'nonce',
      _nonce,
      'outcome',
      _outcome,
      'public_key',
      _public_key,
      'receipt',
      r.receipt,
      'receiver_id',
      _receiver_id,
      'signature',
      _signature,
      'signer_id',
      _signer_id
    ) AS tx,
    _partition_by_block_number,
    t._inserted_timestamp,
    GREATEST(
      t._modified_timestamp,
      b._modified_timestamp) AS _modified_timestamp
  FROM
    int_txs t
    INNER JOIN receipt_array r USING (tx_hash)
    INNER JOIN int_blocks b USING (block_id)
),
{# The following steps were copied directly from legacy tx model to replicate columns #}
actions AS (
  SELECT
    tx_hash,
    SUM(
      VALUE :FunctionCall :gas :: NUMBER
    ) AS attached_gas
  FROM
    base_transactions,
    LATERAL FLATTEN(
      input => tx :actions
    )
  GROUP BY
    1
),
transactions AS (
  SELECT
    block_id,
    tx :outcome :block_hash :: STRING AS block_hash,
    tx_hash,
    block_timestamp,
    tx :nonce :: NUMBER AS nonce,
    tx :signature :: STRING AS signature,
    tx :receiver_id :: STRING AS tx_receiver,
    tx :signer_id :: STRING AS tx_signer,
    tx,
    tx :outcome :outcome :gas_burnt :: NUMBER AS transaction_gas_burnt,
    tx :outcome :outcome :tokens_burnt :: NUMBER AS transaction_tokens_burnt,
    _partition_by_block_number,
    _inserted_timestamp,
    _modified_timestamp
  FROM
    base_transactions
),
gas_burnt AS (
  SELECT
    tx_hash,
    SUM(gas_burnt) AS receipt_gas_burnt,
    SUM(execution_outcome :outcome :tokens_burnt :: NUMBER) AS receipt_tokens_burnt,
    MAX(_modified_timestamp) AS _modified_timestamp
  FROM
    int_receipts
  WHERE
    execution_outcome :outcome: tokens_burnt :: NUMBER != 0
  GROUP BY 
    1
),
determine_tx_status AS (
  SELECT
    DISTINCT tx_hash,
      LAST_VALUE(
      receipt_succeeded
    ) over (
      PARTITION BY tx_hash
      ORDER BY
        block_id ASC
    ) AS tx_succeeded
  FROM
    int_receipts
),
FINAL AS (
  SELECT
    t.block_id,
    t.block_hash,
    t.tx_hash,
    t.block_timestamp,
    t.nonce,
    t.signature,
    t.tx_receiver,
    t.tx_signer,
    t.tx,
    t.transaction_gas_burnt + g.receipt_gas_burnt AS gas_used,
    t.transaction_tokens_burnt + g.receipt_tokens_burnt AS transaction_fee,
    COALESCE(
      actions.attached_gas,
      gas_used
    ) AS attached_gas,
    s.tx_succeeded,
    IFF (
      tx_succeeded,
      'Success',
      'Fail'
    ) AS tx_status, -- DEPRECATE TX_STATUS IN GOLD
    t._partition_by_block_number,
    t._inserted_timestamp,
    GREATEST(
      t._modified_timestamp,
      g._modified_timestamp
    ) AS _modified_timestamp
  FROM
    transactions AS t
    INNER JOIN determine_tx_status s
    ON t.tx_hash = s.tx_hash
    INNER JOIN actions
    ON t.tx_hash = actions.tx_hash
    INNER JOIN gas_burnt g
    ON t.tx_hash = g.tx_hash
)
SELECT
  tx_hash,
  block_id,
  block_hash,
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
  tx_status,
  _partition_by_block_number,
  _inserted_timestamp,
  _modified_timestamp,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash']
  ) }} AS streamline_transactions_final_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL 

