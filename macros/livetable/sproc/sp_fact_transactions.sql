{% macro create_sp_refresh_fact_transactions_live() %}

{% set procedure_sql %}
    
    CREATE SCHEMA IF NOT EXISTS {{ target.database }}.livetable;

    CREATE PROCEDURE IF NOT EXISTS {{ target.database }}.livetable.sp_refresh_fact_transactions_live()
    RETURNS STRING
    LANGUAGE SQL
    AS
    $$
    DECLARE
        -- Configuration
        hybrid_table_name STRING DEFAULT '{{ target.database }}.CORE_LIVE.FACT_TRANSACTIONS';
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
        rows_merged INTEGER := 0;
        rows_deleted INTEGER := 0;
    BEGIN
        
        CREATE SCHEMA IF NOT EXISTS {{ target.database }}.core_live;
        
        CREATE HYBRID TABLE IF NOT EXISTS IDENTIFIER(:hybrid_table_name) ( 
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

        MERGE INTO IDENTIFIER(:hybrid_table_name) AS target
        USING (
            SELECT *
            FROM TABLE({{ target.database }}.livetable.tf_fact_transactions(:start_block_for_udtf,:blocks_to_fetch_buffer))
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
            tx_hash, 
            block_id, 
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
            fact_transactions_id,
            inserted_timestamp, 
            modified_timestamp
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


        rows_merged := SQLROWCOUNT;

        DELETE FROM IDENTIFIER(:hybrid_table_name)
        WHERE  IDENTIFIER(:block_timestamp_column) < (DATEADD('minute', - :pruning_threshold_minutes, CURRENT_TIMESTAMP()))::TIMESTAMP_NTZ(9);

        rows_deleted := SQLROWCOUNT;

        RETURN 'Fetched blocks starting from ' || :start_block_for_udtf || '. Merged ' || :rows_merged || ' new transaction rows.' || 'Deleted' || :rows_deleted || 'rows';

    EXCEPTION
        WHEN OTHER THEN
            RETURN 'ERROR: ' || SQLERRM;
    END
    $$;
{% endset %}


{% if execute %}
    {% do run_query(procedure_sql) %}
    {% do log("Deployed stored procedure: livetable.sp_refresh_fact_transactions_live", info=True) %}
{% endif %}

{% endmacro %}