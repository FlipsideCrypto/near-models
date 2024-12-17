{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = 'tx_hash',
  cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE', '_partition_by_block_number'],
  tags = ['receipt_map','scheduled_core']
) }}
-- depends_on: {{ ref('silver__streamline_blocks') }}
-- depends_on: {{ ref('silver__streamline_transactions') }}
-- depends_on: {{ ref('silver__streamline_receipts_final') }}

{% if execute %}

    {% if is_incremental() and not var("MANUAL_FIX") %}
    {% do log("Incremental and not MANUAL_FIX", info=True) %}
    {% set max_mod_query %}

    SELECT
        MAX(modified_timestamp) modified_timestamp
    FROM
        {{ this }}

    {% endset %}

        {% set max_mod = run_query(max_mod_query) [0] [0] %}
        {% if not max_mod or max_mod == 'None' %}
            {% set max_mod = '2099-01-01' %}
        {% endif %}

        {% do log("max_mod: " ~ max_mod, info=True) %}

        {% set min_block_date_query %}
    SELECT
        MIN(
            block_timestamp :: DATE
        )
    FROM
        (
            SELECT
                MIN(_partition_by_block_number) _partition_by_block_number
            FROM
                {{ ref('silver__streamline_transactions') }} A
            WHERE
                modified_timestamp >= '{{max_mod}}'
            UNION ALL
            SELECT
                MIN(_partition_by_block_number) _partition_by_block_number
            FROM
                {{ ref('silver__streamline_receipts_final') }} A
            WHERE
                modified_timestamp >= '{{max_mod}}'
            UNION ALL
            SELECT
                MIN(_partition_by_block_number) _partition_by_block_number
            FROM
                {{ ref('silver__streamline_blocks') }} A
            WHERE
                modified_timestamp >= '{{max_mod}}'
        ) 
    {% endset %}

        {% set min_partition = run_query(min_block_date_query) [0] [0] %}
        {% if not min_partition or min_partition == 'None' %}
            {% set min_partition = 999999999999 %}
        {% endif %}

        {% do log("min_partition: " ~ min_partition, info=True) %}

    {% endif %}

{% endif %}

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
    modified_timestamp
  FROM
    {{ ref('silver__streamline_transactions') }}

    {% if var('MANUAL_FIX') %}

        WHERE
            {{ partition_load_manual('no_buffer') }}
            
    {% else %}
        WHERE _partition_by_block_number >= '{{min_partition}}'
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
    modified_timestamp
  FROM
    {{ ref('silver__streamline_receipts_final') }}

    {% if var('MANUAL_FIX') %}

        WHERE
            {{ partition_load_manual('end') }}
            
    {% else %}
        WHERE _partition_by_block_number >= '{{min_partition}}'
    {% endif %}

),
int_blocks AS (
  SELECT
    block_id,
    block_hash,
    block_timestamp,
    _partition_by_block_number,
    _inserted_timestamp,
    modified_timestamp
  FROM
    {{ ref('silver__streamline_blocks') }}

    {% if var('MANUAL_FIX') %}

        WHERE
            {{ partition_load_manual('front') }}
            
    {% else %}
        WHERE _partition_by_block_number >= '{{min_partition}}'
    {% endif %}
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
      _actions, -- does not need to exist as col (in receipts) but fields used in gas calc!
      'hash',
      _hash, -- exists as col
      'nonce',
      _nonce, -- exists as col
      'outcome',
      _outcome, -- does not need to exist as col (in receipts) but fields used in gas calc!
      'public_key',
      _public_key, -- does not exist as col
      'receipt',
      r.receipt, -- does not need to exist as col
      'receiver_id',
      _receiver_id, -- exists as col
      'signature',
      _signature, -- exists as col
      'signer_id',
      _signer_id -- exists as col
    ) AS tx, -- TODO dropping this object
    _partition_by_block_number,
    t._inserted_timestamp
  FROM
    int_txs t
    INNER JOIN receipt_array r USING (tx_hash)
    INNER JOIN int_blocks b USING (block_id)

    {% if is_incremental() and not var("MANUAL_FIX") %}
        WHERE
            GREATEST(
                COALESCE(r.modified_timestamp, '1970-01-01'),
                COALESCE(t.modified_timestamp, '1970-01-01'),
                COALESCE(b.modified_timestamp, '1970-01-01')
            ) >= '{{max_mod}}'
    {% endif %}
),
-- TODO review the below section in a subsequent PR. ~3y old at this point.
-- it is calculating the gaas and fees. Largely accurate, but may have found an inaccuracy:
-- Dojw9TnLbTLTxeJAuGtiSTZGdruCAmdu1KsLgjLHwkkb incorrect tx fees / gas burnt?

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

