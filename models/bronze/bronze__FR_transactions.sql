{{ config (
    materialized = var('LIVE_TABLE_MATERIALIZATION', 'view'),
    tags = ['streamline_helper']
) }}

{% if var('ENABLE_LIVE_TABLE', false) %}
    
    {%- set blockchain = this.schema -%}
    {%- set network = this.identifier -%}
    {%- set schema = blockchain ~ "_" ~ network -%}

    WITH spine AS (
        {{ near_live_table_target_blocks(start_block='_block_height', block_count='row_count') | indent(4) -}}
    ),
    raw_blocks AS (
        {{ near_live_table_get_raw_block_data('spine') | indent(4) -}}
    ),
    block_chunk_hashes AS (
        -- Extract block info and the chunk_hash from each chunk header
        SELECT
            rb.block_height,
            rb.rpc_data_result:header:timestamp::STRING AS block_timestamp_str,
            ch.value:chunk_hash::STRING AS chunk_hash,
            ch.value:shard_id::INTEGER AS shard_id,
            ch.value:height_created::INTEGER AS chunk_height_created,
            ch.value:height_included::INTEGER AS chunk_height_included
        FROM raw_blocks rb,
            LATERAL FLATTEN(input => rb.rpc_data_result:chunks) ch
        WHERE ch.value:tx_root::STRING <> '11111111111111111111111111111111' 
    ),
    raw_chunk_details AS (
        -- Fetch full chunk details using the chunk_hash
        SELECT
            bch.block_height,
            bch.block_timestamp_str,
            bch.shard_id,
            bch.chunk_hash,
            bch.chunk_height_created,
            bch.chunk_height_included,
            live_table.lt_chunks_udf_api(
                'POST',
                '{Service}',
                {'Content-Type' : 'application/json'},
                {
                    'jsonrpc' : '2.0',
                    'method' : 'chunk',
                    'id' : 'Flipside/chunk/' || bch.block_height || '/' || bch.chunk_hash,
                    'params': {'chunk_id': bch.chunk_hash}
                },
                _utils.UDF_WHOAMI(),
                'Vault/prod/near/quicknode/mainnet'
            ):data:result AS chunk_data 
        FROM block_chunk_hashes bch
    ),
    chunk_txs AS (
        -- Flatten the transactions array from the actual chunk_data result
        SELECT
            rcd.block_height,
            rcd.block_timestamp_str,
            rcd.shard_id,
            rcd.chunk_hash,
            rcd.chunk_height_created,
            rcd.chunk_height_included,
            tx.value:hash::STRING AS tx_hash,
            tx.value:signer_id::STRING AS tx_signer
        FROM raw_chunk_details rcd,
            LATERAL FLATTEN(input => rcd.chunk_data:transactions) tx 
    ),
    transactions AS (
        SELECT
            DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS request_timestamp,
            tx.block_height,
            tx.block_timestamp_str,
            tx.tx_hash,
            tx.tx_signer,
            tx.shard_id,
            tx.chunk_hash,
            tx.chunk_height_created,
            tx.chunk_height_included,
            live_table.lt_tx_udf_api(
                'POST',
                '{Service}',
                {'Content-Type' : 'application/json', 'fsc-compression-mode' : 'auto'},
                {
                    'jsonrpc' : '2.0',
                    'method' : 'EXPERIMENTAL_tx_status',
                    'id' : 'Flipside/EXPERIMENTAL_tx_status/' || request_timestamp || '/' || tx.block_height :: STRING,
                    'params' : {
                                'tx_hash': tx.tx_hash,
                                'sender_account_id': tx.tx_signer,
                                'wait_until': 'FINAL'
                            }
                },
                _utils.UDF_WHOAMI(),
                'Vault/prod/near/quicknode/mainnet'
            ):data:result AS tx_result
        FROM chunk_txs tx
    )

    SELECT
        tx.tx_result as data,
        {
            'FINAL_EXECUTION_STATUS': tx.tx_result:final_execution_status,
            'RECEIPTS': tx.tx_result:receipts,
            'RECEIPTS_OUTCOME': tx.tx_result:receipts_outcome,
            'STATUS': tx.tx_result:status,
            'TRANSACTION': tx.tx_result:transaction,
            'TRANSACTION_OUTCOME': tx.tx_result:transaction_outcome,
            'BLOCK_ID': tx.block_height,
            'BLOCK_TIMESTAMP_EPOCH': DATE_PART('EPOCH_SECOND', TO_TIMESTAMP_NTZ(tx.block_timestamp_str))::INTEGER,
            'SHARD_ID': tx.shard_id,
            'CHUNK_HASH': tx.chunk_hash,
            'HEIGHT_CREATED': tx.chunk_height_created,
            'HEIGHT_INCLUDED': tx.chunk_height_included
        } as value,
        round(tx.block_height, -3) AS partition_key,
        CURRENT_TIMESTAMP() AS _inserted_timestamp
    FROM transactions tx

{% else %}
    -- BATCH LOGIC: Default
    {{ streamline_external_table_FR_query_v2(
        model = "transactions_v2",
        partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER )") 
    }}
{% endif %}
