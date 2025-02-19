{{ config(
  materialized = 'incremental',
  incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
  incremental_strategy = 'merge',
  merge_exclude_columns = ['inserted_timestamp'],
  unique_key = 'tx_hash',
  cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE', '_partition_by_block_number'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,signer_id,receiver_id);",
  tags = ['scheduled_core']
) }}

WITH
txs_with_receipts as (
select * from {{ ref('silver__streamline_transactions_final') }}
-- TODO incrementally load
),
transaction_full as (
    select
        -- shard_id,
        -- chunk_hash,
        origin_block_id AS block_id,
        origin_block_timestamp AS block_timestamp,
        tx_hash,
        response_json :transaction:actions :: ARRAY as actions,
        response_json :transaction:nonce :: INT as nonce,
        response_json:transaction:priority_fee :: INT as priority_fee,
        response_json:transaction:public_key :: STRING as public_key,
        response_json:transaction:receiver_id :: STRING as receiver_id,
        response_json:transaction:signature :: STRING as signature,
        response_json:transaction:signer_id :: STRING as signer_id,
        response_json:transaction_outcome:block_hash :: STRING as block_hash,
        response_json:transaction_outcome:outcome :: variant as outcome_json,
        response_json:status::VARIANT as status_json
    from txs_with_receipts
)
-- do we still want to calculate total used gas ?
-- well, no need to pull in any other table anymore as the outcome exists in the underlying record
-- can just flatten array and sum values
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
    _inserted_timestamp
  FROM
    base_transactions
),
gas_burnt AS (
  SELECT
    tx_hash,
    SUM(gas_burnt) AS receipt_gas_burnt,
    SUM(execution_outcome :outcome :tokens_burnt :: NUMBER) AS receipt_tokens_burnt
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
    t._inserted_timestamp
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
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash']
  ) }} AS streamline_transactions_final_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL 

