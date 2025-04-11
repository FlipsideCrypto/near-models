{% macro config_near_high_level_abstractions(blockchain, network) -%}
{#
    This macro is used to generate the high level abstractions for the Near
    blockchain.
 #}
{% set schema = blockchain ~ "_" ~ network %}

- name: {{ schema }}.udf_get_latest_block_height
  signature: []
  return_type: INTEGER
  sql: |
    {{ near_live_table_latest_block_height() | indent(4) -}}

- name: {{ schema }}.lt_tx_udf_api
  signature:
    - [method, STRING]
    - [url, STRING]
    - [headers, OBJECT]
    - [DATA, VARIANT]
    - [user_id, STRING]
    - [SECRET, STRING]
  return_type: VARIANT
  func_type: EXTERNAL
  api_integration: '{{ var("API_INTEGRATION") }}'
  options: |
    NOT NULL
    MAX_BATCH_ROWS = 25
  sql: udf_api

- name: {{ schema }}.lt_blocks_udf_api
  signature:
    - [method, STRING]
    - [url, STRING]
    - [headers, OBJECT]
    - [DATA, VARIANT]
    - [user_id, STRING]
    - [SECRET, STRING]
  return_type: VARIANT
  func_type: EXTERNAL
  api_integration: '{{ var("API_INTEGRATION") }}'
  options: |
    NOT NULL
    MAX_BATCH_ROWS = 25
  sql: udf_api

- name: {{ schema }}.lt_chunks_udf_api
  signature:
    - [method, STRING]
    - [url, STRING]
    - [headers, OBJECT]
    - [DATA, VARIANT]
    - [user_id, STRING]
    - [SECRET, STRING]
  return_type: VARIANT
  func_type: EXTERNAL
  api_integration: '{{ var("API_INTEGRATION") }}'
  options: |
    NOT NULL
    MAX_BATCH_ROWS = 25
  sql: udf_api

- name: {{ schema -}}.tf_fact_blocks
  signature:
    - [_block_height, INTEGER, The start block height to get the blocks from]
    - [row_count, INTEGER, The number of rows to fetch]
  return_type:
    - "TABLE(block_id NUMBER, block_timestamp TIMESTAMP_NTZ, block_hash STRING, block_author STRING, header OBJECT, block_challenges_result ARRAY, block_challenges_root STRING, chunk_headers_root STRING, chunk_tx_root STRING, chunk_mask ARRAY, chunk_receipts_root STRING, chunks ARRAY, chunks_included NUMBER, epoch_id STRING, epoch_sync_data_hash STRING, gas_price FLOAT, last_ds_final_block STRING, last_final_block STRING, latest_protocol_version INT, next_bp_hash STRING, next_epoch_id STRING, outcome_root STRING, prev_hash STRING, prev_height NUMBER, prev_state_root STRING, random_value STRING, rent_paid FLOAT, signature STRING, total_supply FLOAT, validator_proposals ARRAY, validator_reward FLOAT, fact_blocks_id STRING, inserted_timestamp TIMESTAMP_NTZ, modified_timestamp TIMESTAMP_NTZ)"
  options: |
    NOT NULL
    RETURNS NULL ON NULL INPUT
    VOLATILE
    COMMENT = $$Returns the block data for a given block height.Fetches blocks for the specified number of blocks $$
  sql: |
    {{ near_live_table_fact_blocks(schema, blockchain, network) | indent(4) -}}
  
  - name: {{ schema -}}.tf_fact_transactions
  signature:
    - [_block_height, INTEGER, The start block height to get the transactions from]
    - [row_count, INTEGER, The number of rows to fetch]
  return_type:
    - "TABLE(tx_hash STRING, block_id NUMBER, block_timestamp TIMESTAMP_NTZ, nonce INT, signature STRING, tx_receiver STRING, tx_signer STRING, tx VARIANT, gas_used NUMBER, transaction_fee NUMBER, attached_gas NUMBER, tx_succeeded BOOLEAN, fact_transactions_id STRING, inserted_timestamp TIMESTAMP_NTZ, modified_timestamp TIMESTAMP_NTZ)"
  options: |
    NOT NULL
    RETURNS NULL ON NULL INPUT
    VOLATILE
    COMMENT = $$Returns transaction details for blocks starting from a given height.Fetches txs for the specified number of blocks.$$
  sql: |
    {{ near_live_table_fact_transactions(schema, blockchain, network) | indent(4) -}}
  
  - name: {{ schema -}}.tf_fact_receipts
  signature:
    - [_block_height, INTEGER, The start block height to get the receipts from]
    - [row_count, INTEGER, The number of rows to fetch]
  return_type:
    - "TABLE(block_timestamp TIMESTAMP_NTZ, block_id NUMBER, tx_hash STRING, receipt_id STRING, receipt_outcome_id ARRAY, receiver_id STRING, predecessor_id STRING, actions VARIANT, outcome VARIANT, gas_burnt NUMBER, status_value VARIANT, logs ARRAY, proof ARRAY, metadata VARIANT, receipt_succeeded BOOLEAN, fact_receipts_id STRING, inserted_timestamp TIMESTAMP_NTZ, modified_timestamp TIMESTAMP_NTZ)"
  options: |
    NOT NULL
    RETURNS NULL ON NULL INPUT
    VOLATILE
    COMMENT = $$Returns receipt details for blocks starting from a given height. Fetches receipts for the specified number of blocks.$$
  sql: |
    {{ near_live_table_fact_receipts(schema, blockchain, network) | indent(4) -}}
  
  - name: {{ schema -}}.tf_ez_actions
  signature:
    - [_block_height, INTEGER, The start block height to get the actions from]
    - [row_count, INTEGER, The number of rows to fetch]
  return_type:
    - "TABLE(block_id NUMBER, block_timestamp TIMESTAMP_NTZ, tx_hash STRING, tx_signer BOOLEAN, tx_receiver STRING, tx_gas_used STRING, tx_succeeded NUMBER, tx_fee NUMBER, receipt_id STRING, receipt_receiver_id STRING, receipt_signer_id STRING, receipt_predecessor_id STRING, receipt_succeeded BOOLEAN, receipt_gas_burnt NUMBER, receipt_status_value OBJECT, is_delegated NUMBER, action_index BOOLEAN, action_name STRING, action_data OBJECT, action_gas_price NUMBER, _partition_by_block_number NUMBER, actions_id STRING, inserted_timestamp TIMESTAMP_NTZ, modified_timestamp TIMESTAMP_NTZ, _invocation_id STRING)"
  options: |
    NOT NULL
    RETURNS NULL ON NULL INPUT
    VOLATILE
    COMMENT = $$Returns decoded action details for blocks starting from a given height. Fetches actions for the specified number of blocks.$$
  sql: |
    {{ near_live_table_ez_actions(schema, blockchain, network) | indent(4) -}}

{%- endmacro -%}

