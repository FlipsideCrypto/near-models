{% macro create_sp_refresh_fact_transactions_live() %}

{% set procedure_sql %}
    
    CREATE SCHEMA IF NOT EXISTS {{ target.database }}.livetable;
    CREATE SCHEMA IF NOT EXISTS {{ target.database }}.core_live;
    CREATE SCHEMA IF NOT EXISTS {{ target.database }}.bronze_live;

    CREATE PROCEDURE IF NOT EXISTS {{ target.database }}.livetable.sp_refresh_fact_transactions_live()
    RETURNS STRING
    LANGUAGE SQL
    AS
    $$
    DECLARE
        -- Configuration
        gold_hybrid_table_name STRING DEFAULT '{{ target.database }}.CORE_LIVE.FACT_TRANSACTIONS';
        bronze_rpc_responses_table_name STRING DEFAULT '{{ target.database }}.BRONZE_LIVE.TRANSACTIONS';
        udtf_name STRING DEFAULT '{{ target.database }}.LIVETABLE.TF_FACT_TRANSACTIONS';
        chain_head_udf STRING DEFAULT '_live.udf_api'; 
        secret_path STRING DEFAULT 'Vault/prod/near/quicknode/livetable/mainnet';
        pk_column STRING DEFAULT 'tx_hash';
        blocks_to_fetch_buffer INTEGER DEFAULT 385;
        block_timestamp_column STRING DEFAULT 'block_timestamp';
        pruning_threshold_minutes INTEGER DEFAULT 60;

        -- State Variables
        chain_head_block INTEGER;
        start_block_for_udtf INTEGER;
        rows_merged_gold INTEGER := 0;
        rows_merged_bronze_stage INTEGER := 0;
        rows_deleted INTEGER := 0;
        temp_table_name STRING;
        final_return_message STRING;
        error_message STRING;

    BEGIN
        
        CREATE HYBRID TABLE IF NOT EXISTS IDENTIFIER(:gold_hybrid_table_name) ( 
            tx_hash STRING PRIMARY KEY, 
            block_id NUMBER, 
            block_timestamp TIMESTAMP_NTZ, 
            nonce INT,
            signature STRING, 
            tx_receiver STRING, 
            tx_signer STRING, 
            tx VARIANT, 
            gas_used NUMBER,
            transaction_fee NUMBER, 
            attached_gas NUMBER, 
            tx_succeeded BOOLEAN, 
            fact_transactions_id STRING,
            inserted_timestamp TIMESTAMP_NTZ, 
            modified_timestamp TIMESTAMP_NTZ,

            INDEX idx_tx_signer (tx_signer),
            INDEX idx_tx_receiver (tx_receiver)
        );

        CREATE HYBRID TABLE IF NOT EXISTS IDENTIFIER(:bronze_rpc_responses_table_name) (
            TX_HASH STRING PRIMARY KEY,
            PARTITION_KEY NUMBER,
            RPC_RESPONSE_DATA VARIANT,
            SHARD_ID NUMBER,
            CHUNK_HASH STRING,
            BRONZE_BLOCK_ID NUMBER,
            BLOCK_TIMESTAMP_EPOCH NUMBER,
            HEIGHT_CREATED NUMBER,
            HEIGHT_INCLUDED NUMBER,
            REQUEST_TIMESTAMP NUMBER DEFAULT (DATE_PART('EPOCH_SECOND', SYSDATE())::NUMBER),
            METADATA VARIANT DEFAULT NULL,
 
            INDEX idx_rpc_request_ts (REQUEST_TIMESTAMP)
        );

        SELECT IDENTIFIER(:chain_head_udf)(
                'POST',
                '{Service}', 
                {'Content-Type': 'application/json', 'fsc-compression-mode': 'auto'},
                {'jsonrpc': '2.0', 'method': 'block', 'id': 'Flipside/block/' || DATE_PART('EPOCH', SYSDATE()) :: STRING, 'params': {'finality': 'final'}},
                _utils.UDF_WHOAMI(),
                :secret_path
            ):data:result:header:height::INTEGER
        INTO :chain_head_block; 

        IF (:chain_head_block IS NULL) THEN
            RETURN 'ERROR: Failed to fetch chain head block height.';
        END IF;

        start_block_for_udtf := :chain_head_block - :blocks_to_fetch_buffer + 1;

        temp_table_name := '{{ target.database }}.LIVETABLE.UDTF_TX_OUTPUT_TEMP_' || REPLACE(UUID_STRING(),'-','_');

        CREATE TEMPORARY TABLE IDENTIFIER(:temp_table_name)
            AS SELECT * FROM TABLE({{ target.database }}.LIVETABLE.TF_FACT_TRANSACTIONS(:start_block_for_udtf, :blocks_to_fetch_buffer));

        MERGE INTO IDENTIFIER(:gold_hybrid_table_name) AS target
        USING (
            SELECT
                TX_HASH, BLOCK_ID, BLOCK_TIMESTAMP, NONCE, SIGNATURE, TX_RECEIVER, TX_SIGNER, TX,
                GAS_USED, TRANSACTION_FEE, ATTACHED_GAS, TX_SUCCEEDED, FACT_TRANSACTIONS_ID,
                INSERTED_TIMESTAMP, MODIFIED_TIMESTAMP

            FROM IDENTIFIER(:temp_table_name)
        ) AS source
        ON target.tx_hash = source.tx_hash
        WHEN MATCHED THEN UPDATE SET
            target.block_id = source.block_id, 
            target.block_timestamp = source.block_timestamp, 
            target.nonce = source.nonce,
            target.signature = source.signature, 
            target.tx_receiver = source.tx_receiver, 
            target.tx_signer = source.tx_signer,
            target.tx = source.tx, 
            target.gas_used = source.gas_used, 
            target.transaction_fee = source.transaction_fee,
            target.attached_gas = source.attached_gas, 
            target.tx_succeeded = source.tx_succeeded,
            target.fact_transactions_id = source.fact_transactions_id, 
            target.inserted_timestamp = source.inserted_timestamp,
            target.modified_timestamp = source.modified_timestamp
        WHEN NOT MATCHED THEN INSERT (
            tx_hash, block_id, block_timestamp, nonce, signature, tx_receiver, tx_signer, tx,
            gas_used, transaction_fee, attached_gas, tx_succeeded, fact_transactions_id,
            inserted_timestamp, modified_timestamp
        ) VALUES (
            source.tx_hash, 
            source.block_id, 
            source.block_timestamp, 
            source.nonce, 
            source.signature, 
            source.tx_receiver, 
            source.tx_signer, 
            source.tx,
            source.gas_used, 
            source.transaction_fee, 
            source.attached_gas, 
            source.tx_succeeded, 
            source.fact_transactions_id,
            source.inserted_timestamp, 
            source.modified_timestamp
        );

        rows_merged_gold := SQLROWCOUNT;

        DELETE FROM IDENTIFIER(:gold_hybrid_table_name)
        WHERE  IDENTIFIER(:block_timestamp_column) < (DATEADD('minute', - :pruning_threshold_minutes, CURRENT_TIMESTAMP()))::TIMESTAMP_NTZ(9);

        rows_deleted := SQLROWCOUNT;

        MERGE INTO IDENTIFIER(:bronze_rpc_responses_table_name) AS target
        USING (
            SELECT
                TX_HASH,
                PARTITION_KEY,
                DATA AS RPC_RESPONSE_DATA,
                VALUE:SHARD_ID::NUMBER AS SHARD_ID,
                VALUE:CHUNK_HASH::STRING AS CHUNK_HASH,
                VALUE:BLOCK_ID::NUMBER AS BRONZE_BLOCK_ID,
                VALUE:BLOCK_TIMESTAMP_EPOCH::NUMBER AS BLOCK_TIMESTAMP_EPOCH,
                VALUE:HEIGHT_CREATED::NUMBER AS HEIGHT_CREATED,
                VALUE:HEIGHT_INCLUDED::NUMBER AS HEIGHT_INCLUDED,
                TX_SIGNER
                
            FROM IDENTIFIER(:temp_table_name)
            WHERE TX_HASH IS NOT NULL
        ) AS source
        ON target.tx_hash = source.tx_hash
        WHEN MATCHED THEN UPDATE SET
            target.PARTITION_KEY = source.PARTITION_KEY,
            target.RPC_RESPONSE_DATA = source.RPC_RESPONSE_DATA,
            target.SHARD_ID = source.SHARD_ID,
            target.CHUNK_HASH = source.CHUNK_HASH,
            target.BRONZE_BLOCK_ID = source.BRONZE_BLOCK_ID,
            target.BLOCK_TIMESTAMP_EPOCH = source.BLOCK_TIMESTAMP_EPOCH,
            target.HEIGHT_CREATED = source.HEIGHT_CREATED,
            target.HEIGHT_INCLUDED = source.HEIGHT_INCLUDED,
            target.REQUEST_TIMESTAMP = (DATE_PART('EPOCH_SECOND', SYSDATE())::NUMBER),
            target.METADATA = {
                'app_name': 'livetable_sproc',
                'batch_id': NULL, 
                'request_id': NULL,
                'request': {
                    'data': {
                        'id': 'Flipside/EXPERIMENTAL_tx_status/' || target.REQUEST_TIMESTAMP::STRING || '/' || source.TX_HASH,
                        'jsonrpc': '2.0',
                        'method': 'EXPERIMENTAL_tx_status',
                        'params': {
                            'sender_account_id': source.TX_SIGNER,
                            'tx_hash': source.TX_HASH,
                            'wait_until': 'FINAL'
                        }
                    },
                    'headers': {
                        'Content-Type': 'application/json',
                        'fsc-compression-mode': 'auto'
                    },
                    'method': 'POST',
                    'secret_name': :secret_path,
                    'url': '{Service}'
                }
            }
        WHEN NOT MATCHED THEN INSERT (
            TX_HASH,
            PARTITION_KEY,
            RPC_RESPONSE_DATA,
            SHARD_ID,
            CHUNK_HASH,
            BRONZE_BLOCK_ID,
            BLOCK_TIMESTAMP_EPOCH,
            HEIGHT_CREATED,
            HEIGHT_INCLUDED,
            METADATA
            -- REQUEST_TIMESTAMP will use DEFAULT
        ) VALUES (
            source.TX_HASH,
            source.PARTITION_KEY,
            source.RPC_RESPONSE_DATA,
            source.SHARD_ID,
            source.CHUNK_HASH,
            source.BRONZE_BLOCK_ID,
            source.BLOCK_TIMESTAMP_EPOCH,
            source.HEIGHT_CREATED,
            source.HEIGHT_INCLUDED,
            { 
                'app_name': 'livetable_sproc',
                'batch_id': NULL,
                'request_id': NULL,
                'request': {
                    'data': {
                        'id': 'Flipside/EXPERIMENTAL_tx_status/' || (DATE_PART('EPOCH_SECOND', SYSDATE())::NUMBER)::STRING || '/' || source.TX_HASH,
                        'jsonrpc': '2.0',
                        'method': 'EXPERIMENTAL_tx_status',
                        'params': {
                            'sender_account_id': source.TX_SIGNER,
                            'tx_hash': source.TX_HASH,
                            'wait_until': 'FINAL'
                        }
                    },
                    'headers': {
                        'Content-Type': 'application/json',
                        'fsc-compression-mode': 'auto'
                    },
                    'method': 'POST',
                    'secret_name': :secret_path,
                    'url': '{Service}'
                }
            }

        );

        rows_merged_bronze_stage := SQLROWCOUNT;

        DROP TABLE IDENTIFIER(:temp_table_name);

        final_return_message := 'Gold: Merged ' || :rows_merged_gold || ', Pruned ' || :rows_deleted_gold || '. ' ||
                                'Bronze Stage: Upserted ' || :rows_merged_bronze_stage || ' RPC responses.';
        RETURN final_return_message;

    EXCEPTION
        WHEN OTHER THEN
            error_message := 'ERROR in sp_refresh_fact_transactions_live: ' || SQLERRM;
            BEGIN
                DROP TABLE IF EXISTS IDENTIFIER(:temp_table_name);
            EXCEPTION WHEN OTHER THEN null;
            END;
            RETURN error_message;
    END
    $$;
{% endset %}


{% if execute %}
    {% do run_query(procedure_sql) %}
    {% do log(procedure_sql, info=True) %}
    {% do log("Deployed stored procedure: livetable.sp_refresh_fact_transactions_live", info=True) %}
{% endif %}

{% endmacro %}


