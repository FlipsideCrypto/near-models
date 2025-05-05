{% macro create_sp_refresh_fact_blocks_live() %}

{% set procedure_sql %}
CREATE OR REPLACE PROCEDURE {{ target.database }}.livetable.sp_refresh_fact_blocks_live()
RETURNS STRING
LANGUAGE SQL
AS
$$ 
DECLARE
    -- Configuration
    hybrid_table_name STRING DEFAULT '{{ target.database }}.CORE_LIVE.HYBRID_FACT_BLOCKS'; 
    udtf_name STRING DEFAULT '{{ target.database }}.LIVETABLE.TF_FACT_BLOCKS';         
    chain_head_udf STRING DEFAULT '_live.udf_api';                                 
    secret_path STRING DEFAULT 'Vault/prod/near/quicknode/livetable/mainnet';       
    pk_column STRING DEFAULT 'block_id';                                            
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
        block_id NUMBER PRIMARY KEY,             
        block_timestamp TIMESTAMP_NTZ,          
        block_hash STRING,                      
        block_author STRING,                    
        header OBJECT,                          
        block_challenges_result ARRAY,          
        block_challenges_root STRING,           
        chunk_headers_root STRING,              
        chunk_tx_root STRING,                   
        chunk_mask ARRAY,                       
        chunk_receipts_root STRING,             
        chunks ARRAY,                           
        chunks_included NUMBER,                 
        epoch_id STRING,                        
        epoch_sync_data_hash STRING,            
        gas_price FLOAT,                         
        last_ds_final_block STRING,             
        last_final_block STRING,                
        latest_protocol_version INT,            
        next_bp_hash STRING,                    
        next_epoch_id STRING,                   
        outcome_root STRING,                    
        prev_hash STRING,                        
        prev_height NUMBER,                     
        prev_state_root STRING,                 
        random_value STRING,                    
        rent_paid FLOAT,                        
        signature STRING,                       
        total_supply FLOAT,                      
        validator_proposals ARRAY,              
        validator_reward FLOAT,                 
        fact_blocks_id STRING,                   
        inserted_timestamp TIMESTAMP_NTZ,        
        modified_timestamp TIMESTAMP_NTZ,        
        _hybrid_updated_at TIMESTAMP_LTZ DEFAULT SYSDATE()
    );

    SELECT IDENTIFIER(:chain_head_udf)(
               'POST', 
               '{Service}', 
               {
                    'Content-Type': 'application/json',
                    'fsc-compression-mode': 'auto'
               },
               {
                'jsonrpc': '2.0', 
                'id': 'Flipside/block/' || DATE_PART('EPOCH', SYSDATE()) :: STRING, 
                'method': 'block', 
                'params': {'finality': 'final'}
               },
               _utils.UDF_WHOAMI(), :secret_path
           ):data:result:header:height::INTEGER
    INTO :chain_head_block;

    IF (:chain_head_block IS NULL) THEN
            RETURN 'ERROR: Failed to fetch chain head block height.';
    END IF;

    start_block_for_udtf := :chain_head_block - :blocks_to_fetch_buffer + 1;

    MERGE INTO IDENTIFIER(:hybrid_table_name) AS target
    USING (
        SELECT * FROM TABLE(IDENTIFIER(:udtf_name)(:start_block_for_udtf, :blocks_to_fetch_buffer))
    ) AS source
    ON target.block_id = source.block_id
    WHEN MATCHED THEN UPDATE SET
        target.block_timestamp = source.block_timestamp, target.block_hash = source.block_hash,
        target.block_author = source.block_author, target.header = source.header,
        target.block_challenges_result = source.block_challenges_result, target.block_challenges_root = source.block_challenges_root,
        target.chunk_headers_root = source.chunk_headers_root, target.chunk_tx_root = source.chunk_tx_root,
        target.chunk_mask = source.chunk_mask, target.chunk_receipts_root = source.chunk_receipts_root,
        target.chunks = source.chunks, target.chunks_included = source.chunks_included,
        target.epoch_id = source.epoch_id, target.epoch_sync_data_hash = source.epoch_sync_data_hash,
        target.gas_price = source.gas_price, target.last_ds_final_block = source.last_ds_final_block,
        target.last_final_block = source.last_final_block, target.latest_protocol_version = source.latest_protocol_version,
        target.next_bp_hash = source.next_bp_hash, target.next_epoch_id = source.next_epoch_id,
        target.outcome_root = source.outcome_root, target.prev_hash = source.prev_hash,
        target.prev_height = source.prev_height, target.prev_state_root = source.prev_state_root,
        target.random_value = source.random_value, target.rent_paid = source.rent_paid,
        target.signature = source.signature, target.total_supply = source.total_supply,
        target.validator_proposals = source.validator_proposals, target.validator_reward = source.validator_reward,
        target.fact_blocks_id = source.fact_blocks_id, target.inserted_timestamp = source.inserted_timestamp,
        target.modified_timestamp = source.modified_timestamp,
        target._hybrid_updated_at = SYSDATE()
    WHEN NOT MATCHED THEN INSERT (

        block_id, block_timestamp, block_hash, block_author, header, block_challenges_result,
        block_challenges_root, chunk_headers_root, chunk_tx_root, chunk_mask,
        chunk_receipts_root, chunks, chunks_included, epoch_id, epoch_sync_data_hash,
        gas_price, last_ds_final_block, last_final_block, latest_protocol_version,
        next_bp_hash, next_epoch_id, outcome_root, prev_hash, prev_height,
        prev_state_root, random_value, rent_paid, signature, total_supply,
        validator_proposals, validator_reward, fact_blocks_id, inserted_timestamp,
        modified_timestamp, _hybrid_updated_at
    ) VALUES (
        source.block_id, source.block_timestamp, source.block_hash, source.block_author, source.header, source.block_challenges_result,
        source.block_challenges_root, source.chunk_headers_root, source.chunk_tx_root, source.chunk_mask,
        source.chunk_receipts_root, source.chunks, source.chunks_included, source.epoch_id, source.epoch_sync_data_hash,
        source.gas_price, source.last_ds_final_block, source.last_final_block, source.latest_protocol_version,
        source.next_bp_hash, source.next_epoch_id, source.outcome_root, source.prev_hash, source.prev_height,
        source.prev_state_root, source.random_value, source.rent_paid, source.signature, source.total_supply,
        source.validator_proposals, source.validator_reward, source.fact_blocks_id, source.inserted_timestamp,
        source.modified_timestamp, SYSDATE()
    );
    rows_merged := SQLROWCOUNT;

    -- 4. Prune Hybrid Table (Same logic, different table)
    DELETE FROM IDENTIFIER(:hybrid_table_name)
    WHERE IDENTIFIER(:block_timestamp_column) < (DATEADD('minute', - :pruning_threshold_minutes, CURRENT_TIMESTAMP()))::TIMESTAMP_NTZ(9);
    rows_deleted := SQLROWCOUNT;

    RETURN 'Refreshed Blocks: Fetched starting ' || :start_block_for_udtf || '. Merged ' || :rows_merged || ' rows. Pruned ' || :rows_deleted || ' rows.';

EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR in sp_refresh_fact_blocks_live: ' || SQLERRM;
END;
$$; 
{% endset %}


{% if execute %}
    {% do run_query(procedure_sql) %}
    {% do log("Created sp_refresh_fact_blocks_live", info=True) %}
{% else %}
    {% do log("Skipping sp_refresh_fact_blocks_live creation during compile phase.", info=True) %}
{% endif %}

{% endmacro %}