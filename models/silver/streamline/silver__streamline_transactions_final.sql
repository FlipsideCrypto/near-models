{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  merge_exclude_columns = ['inserted_timestamp'],
  unique_key = 'tx_hash',
  cluster_by = ['_inserted_timestamp::date', '_modified_timestamp::DATE', '_partition_by_block_number'],
  tags = ['receipt_map']
) }}

{# TODO - need help thinking thru if incr loading should be changed #}

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

    {% if var('IS_MIGRATION') %}
    WHERE
      {{ partition_incremental_load(
        150000,
        10000,
        0
      ) }}
    {% else %}
    WHERE
      {{ partition_incremental_load(
        2000,
        1000,
        0
      ) }}
    {% endif %}
),
int_receipts AS (
  SELECT
    block_id,
    block_timestamp,
    tx_hash,
    execution_outcome,
    receipt_succeeded,
    gas_burnt,
    _partition_by_block_number,
    _inserted_timestamp,
    modified_timestamp AS _modified_timestamp
  FROM
    {{ ref('silver__streamline_receipts_final') }}

    {% if var('IS_MIGRATION') %}
    WHERE
      {{ partition_incremental_load(
        150000,
        10000,
        0
      ) }}
    {% else %}
      {{ partition_incremental_load(
        2000,
        1000,
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
    t._modified_timestamp
  FROM
    int_txs t
    LEFT JOIN receipt_array r USING (tx_hash)
    LEFT JOIN int_blocks b USING (block_id)
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
receipts AS (
  SELECT
    block_id,
    tx_hash,
    receipt_succeeded,
    SUM(
      gas_burnt
    ) over (
      PARTITION BY tx_hash
      ORDER BY
        tx_hash DESC
    ) AS receipt_gas_burnt,
    SUM(
      execution_outcome :outcome :tokens_burnt :: NUMBER
    ) over (
      PARTITION BY tx_hash
      ORDER BY
        tx_hash DESC
    ) AS receipt_tokens_burnt,
    execution_outcome :outcome: tokens_burnt :: NUMBER AS tokens_burnt,
    _modified_timestamp
  FROM
    int_receipts
  WHERE
    tokens_burnt != 0
),
FINAL AS (
  SELECT
    t.block_id,
    t.block_hash,
    t.tx_hash,
    block_timestamp,
    t.nonce,
    t.signature,
    t.tx_receiver,
    t.tx_signer,
    t.tx,
    t.transaction_gas_burnt + r.receipt_gas_burnt AS gas_used,
    t.transaction_tokens_burnt + r.receipt_tokens_burnt AS transaction_fee,
    COALESCE(
      actions.attached_gas,
      gas_used
    ) AS attached_gas,
    LAST_VALUE(
      r.receipt_succeeded
    ) over (
      PARTITION BY r.tx_hash
      ORDER BY
        r.block_id ASC
    ) = TRUE AS tx_succeeded,
    IFF (
      tx_succeeded,
      'Success',
      'Fail'
    ) AS tx_status, -- DEPRECATE TX_STATUS IN GOLD
      _partition_by_block_number,
    _inserted_timestamp,
    GREATEST(
      t._modified_timestamp,
      r._modified_timestamp,
      b._modified_timestamp
    ) AS _modified_timestamp -- TODO - confirm use greatest? Migrating incr logic to modified??
  FROM
    transactions AS t
    LEFT JOIN receipts AS r
    ON t.tx_hash = r.tx_hash
    LEFT JOIN actions
    ON t.tx_hash = actions.tx_hash
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

