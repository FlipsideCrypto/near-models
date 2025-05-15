{% macro create_sp_refresh_bronze_transactions_live() %}

{% set procedure_sql %}
    
    CREATE SCHEMA IF NOT EXISTS {{ target.database }}.livetable;
    CREATE SCHEMA IF NOT EXISTS {{ target.database }}.bronze_live;

    CREATE OR REPLACE PROCEDURE {{ target.database }}.livetable.sp_refresh_bronze_transactions_live()
    RETURNS STRING
    LANGUAGE SQL
    AS
    $$
    DECLARE
        -- Configuration
        bronze_transactions_table_name STRING DEFAULT '{{ target.database }}.BRONZE_LIVE.TRANSACTIONS';
        chain_head_udf STRING DEFAULT '_live.udf_api'; 
        secret_path STRING DEFAULT 'Vault/prod/near/quicknode/livetable/mainnet';
        pk_column STRING DEFAULT 'tx_hash';
        blocks_to_fetch_buffer INTEGER DEFAULT 1;
        block_timestamp_column STRING DEFAULT 'block_timestamp';
        pruning_threshold_minutes INTEGER DEFAULT 60;

        -- State Variables
        chain_head_block INTEGER;
        start_block_for_udtf INTEGER;
        rows_merged_bronze_stage INTEGER := 0;
        final_return_message STRING;
        error_message STRING;

    BEGIN

        CREATE HYBRID TABLE IF NOT EXISTS IDENTIFIER(:bronze_transactions_table_name) (
            TX_HASH STRING PRIMARY KEY,
            PARTITION_KEY NUMBER,
            DATA VARIANT,
            VALUE VARIANT,
            _INSERTED_TIMESTAMP TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(9),
            METADATA VARIANT DEFAULT NULL
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

        MERGE INTO IDENTIFIER(:bronze_transactions_table_name) AS target
        USING (
            SELECT
                DATA :transaction :hash :: STRING AS TX_HASH,
                PARTITION_KEY,
                DATA,
                VALUE,
                _INSERTED_TIMESTAMP
            FROM TABLE({{ target.database }}.LIVETABLE.TF_BRONZE_TRANSACTIONS(:start_block_for_udtf, :blocks_to_fetch_buffer))
        ) AS source
        ON target.tx_hash = source.tx_hash
        WHEN MATCHED THEN UPDATE SET
            target.PARTITION_KEY = source.PARTITION_KEY,
            target.DATA = source.DATA,
            target.VALUE = source.value,
            target._INSERTED_TIMESTAMP = CURRENT_TIMESTAMP(9),
            target.METADATA = {
                'app_name': 'livetable_sproc',
                'batch_id': NULL, 
                'request_id': NULL,
                'request': {
                    'data': {
                        'id': 'Flipside/EXPERIMENTAL_tx_status/' || target._INSERTED_TIMESTAMP::STRING || '/' || source.TX_HASH,
                        'jsonrpc': '2.0',
                        'method': 'EXPERIMENTAL_tx_status',
                        'params': {
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
            DATA,
            VALUE,
            _INSERTED_TIMESTAMP,
            METADATA
        ) VALUES (
            source.TX_HASH,
            source.PARTITION_KEY,
            source.DATA,
            source.value,
            source._INSERTED_TIMESTAMP,
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


        final_return_message := 'Bronze Stage: Upserted ' || :rows_merged_bronze_stage || ' RPC responses.';
        
        RETURN final_return_message;

    EXCEPTION
        WHEN OTHER THEN
            error_message := 'ERROR in sp_refresh_bronze_transactions_live: ' || SQLERRM;
            RETURN error_message;
    END
    $$;
{% endset %}


{% if execute %}
    {% do run_query(procedure_sql) %}
    {% do log(procedure_sql, info=True) %}
    {% do log("Deployed stored procedure: livetable.sp_refresh_bronze_transactions_live", info=True) %}
{% endif %}

{% endmacro %}


