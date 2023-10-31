

WITH int_txs AS (

  SELECT
    *
  FROM
    NEAR_DEV.silver.streamline_transactions

    
where tx_hash in ('F8aLtLN3MQ87kcWfsW9GWzowj1cGUondMT8xyZhY1kPr', '5xec7bgSw45zUzQws3rufYYu3bpNB6gxVtyNy8teew7i')
and _partition_by_block_number between 65000000 and 76000000

    
),
int_receipts AS (
  SELECT
    *
  FROM
    NEAR_DEV.silver.streamline_receipts_final

where tx_hash in ('F8aLtLN3MQ87kcWfsW9GWzowj1cGUondMT8xyZhY1kPr', '5xec7bgSw45zUzQws3rufYYu3bpNB6gxVtyNy8teew7i')
and _partition_by_block_number between 65000000 and 76000000

    
),
int_blocks AS (
  SELECT
    *
  FROM
    NEAR_DEV.silver.streamline_blocks
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
    _load_timestamp,
    _partition_by_block_number,
    t._inserted_timestamp
  FROM
    int_txs t
    LEFT JOIN receipt_array r USING (tx_hash)
    LEFT JOIN int_blocks b USING (block_id)
),

actions AS (
  SELECT
    tx_hash,
    SUM(
      VALUE :FunctionCall :gas
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
    _load_timestamp,
    _partition_by_block_number,
    _inserted_timestamp
  FROM
    base_transactions
),
receipts AS (
  SELECT
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
    ) AS receipt_tokens_burnt
  FROM
    int_receipts
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
    _load_timestamp,
    _partition_by_block_number,
    _inserted_timestamp,
    COALESCE(
      actions.attached_gas,
      gas_used
    ) AS attached_gas,
    LAST_VALUE(
      r.receipt_succeeded
    ) over (
      PARTITION BY r.tx_hash
      ORDER BY
        r.receipt_succeeded DESC
    ) AS tx_status
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
  _load_timestamp,
  _partition_by_block_number,
  attached_gas,
  tx_status,
  _inserted_timestamp
FROM
  FINAL qualify ROW_NUMBER() over (
    PARTITION BY tx_hash
    ORDER BY
      _inserted_timestamp DESC
  ) = 1