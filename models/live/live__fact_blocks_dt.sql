
{{ config(
    materialized='dynamic_table',
    refresh_interval='5 minutes',
    target_lag='1 minute',
    snowflake_warehouse='DBT_CLOUD',  
    transient=false        
) }}


WITH max_gold_block AS (
    
    SELECT
        COALESCE(MAX(block_id), 0) AS max_block_id
    FROM {{ ref('silver__blocks_final') }}
),
chain_head AS (
    SELECT
        DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS request_timestamp,
        _live.udf_api(
                'POST',
                '{Service}',
                {'Content-Type' : 'application/json', 'fsc-compression-mode' : 'auto'},
                {
                    'jsonrpc' : '2.0',
                    'method' : 'block',
                    'id' : 'Flipside/block/' || request_timestamp,
                    'params' : {'finality' : 'final'}
                },
                'SHAH', -- TODO: Replace with _utils.UDF_WHOAMI()
                'Vault/prod/near/quicknode/mainnet'
        ):data:result:header:height::INTEGER AS latest_block_id
),
fetch_parameters AS (
    SELECT
        mgb.max_block_id + 1 AS start_block_id,
        -- TODO: Replace with GREATEST()
        LEAST(ch.latest_block_id - mgb.max_block_id, 10)::INTEGER AS num_rows_to_fetch
    FROM max_gold_block mgb, chain_head ch
),

live_blocks_call AS (
    WITH __dbt__cte__bronze__blocks AS (
        -- LIVE LOGIC: Call RPCs to populate live table
        SELECT 1
    ),

    __dbt__cte__bronze__FR_blocks AS (
        WITH spine AS (
            
            
            WITH heights AS (
                SELECT
                    fp.start_block_id AS min_height,
                    (fp.start_block_id + fp.num_rows_to_fetch - 1) AS max_height,
                    fp.num_rows_to_fetch 
                FROM
                    fetch_parameters fp
            ),
            block_spine AS (
                SELECT
                    ROW_NUMBER() OVER (
                        ORDER BY
                            NULL
                    ) - 1 + h.min_height::integer AS block_number
                FROM
                    heights h,
                    TABLE(generator(ROWCOUNT => 10 ))
                qualify block_number BETWEEN h.min_height AND h.max_height
            )
            SELECT
                block_number as block_height
            FROM block_spine
        ),
        raw_blocks AS (
         
        SELECT
            block_height,
            DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS request_timestamp,
            live_table.lt_blocks_udf_api(
                'POST',
                '{Service}',
                {'Content-Type' : 'application/json'},
                {
                    'jsonrpc' : '2.0',
                    'method' : 'block',
                    'id' : 'Flipside/getBlock/' || request_timestamp || '/' || block_height :: STRING,
                    'params':{'block_id': block_height}
                },
                'SHAH',
                'Vault/prod/near/quicknode/mainnet'
            ):data.result AS rpc_data_result
        from
            spine

    )

    SELECT
        OBJECT_INSERT(
            rb.rpc_data_result,          
            'BLOCK_ID',                 
            rb.block_height,             
            TRUE                         
        ) AS value,
        rb.rpc_data_result AS data,
        round(rb.block_height, -3) AS partition_key,
        CURRENT_TIMESTAMP() AS _inserted_timestamp
    FROM
        raw_blocks rb
),

__dbt__cte__silver__blocks_v2 AS (

WITH bronze_blocks AS (

    SELECT
        VALUE :BLOCK_ID :: INT AS block_id,
        DATA :header :hash :: STRING AS block_hash,
        DATA :header :timestamp :: INT AS block_timestamp_epoch,
        partition_key,
        DATA :: variant AS block_json,
        _inserted_timestamp
    FROM


            __dbt__cte__bronze__FR_blocks
        WHERE
            typeof(DATA) != 'NULL_VALUE'
        
    )
SELECT
    block_id,
    block_hash,
    block_timestamp_epoch,
    TO_TIMESTAMP_NTZ(block_timestamp_epoch, 9) AS block_timestamp,
    partition_key,
    block_json,
    _inserted_timestamp,
    
    
md5(cast(coalesce(cast(block_hash as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS blocks_v2_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '62209d74-945d-4832-9b2f-23aaae0809ae' AS _invocation_id
FROM
    bronze_blocks 

qualify ROW_NUMBER() over (
        PARTITION BY block_hash
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
),

__dbt__cte__silver__blocks_final AS (
    WITH blocks AS (
    SELECT
        block_id,
        block_timestamp,
        block_hash,
        block_json :header :prev_hash :: STRING AS prev_hash,
        block_json :author :: STRING AS block_author,
        block_json :chunks :: ARRAY AS chunks_json,
        block_json :header :: OBJECT AS header_json,
        partition_key AS _partition_by_block_number
    FROM
        __dbt__cte__silver__blocks_v2

        
)
SELECT
    *,
    
    
md5(cast(coalesce(cast(block_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS blocks_final_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '62209d74-945d-4832-9b2f-23aaae0809ae' AS _invocation_id
FROM
    blocks
),

__dbt__cte__core__fact_blocks AS (
    WITH blocks AS (

    SELECT
        *
    FROM
        __dbt__cte__silver__blocks_final
)
SELECT
    block_id,
    block_timestamp,
    block_hash,
    block_author,
    header_json AS header,
    header_json :challenges_result :: ARRAY AS block_challenges_result,
    header_json :challenges_root :: STRING AS block_challenges_root,
    header_json :chunk_headers_root :: STRING AS chunk_headers_root,
    header_json :chunk_tx_root :: STRING AS chunk_tx_root,
    header_json :chunk_mask :: ARRAY AS chunk_mask,
    header_json :chunk_receipts_root :: STRING AS chunk_receipts_root,
    chunks_json AS chunks,
    header_json :chunks_included :: NUMBER AS chunks_included,
    header_json :epoch_id :: STRING AS epoch_id,
    header_json :epoch_sync_data_hash :: STRING AS epoch_sync_data_hash,
    header_json :gas_price :: FLOAT AS gas_price,
    header_json :last_ds_final_block :: STRING AS last_ds_final_block,
    header_json :last_final_block :: STRING AS last_final_block,
    header_json :latest_protocol_version :: INT AS latest_protocol_version,
    header_json :next_bp_hash :: STRING AS next_bp_hash,
    header_json :next_epoch_id :: STRING AS next_epoch_id,
    header_json :outcome_root :: STRING AS outcome_root,
    prev_hash,
    header_json :prev_height :: NUMBER AS prev_height,
    header_json :prev_state_root :: STRING AS prev_state_root,
    header_json :random_value :: STRING AS random_value,
    header_json :rent_paid :: FLOAT AS rent_paid,
    header_json :signature :: STRING AS signature,
    header_json :total_supply :: FLOAT AS total_supply,
    header_json :validator_proposals :: ARRAY AS validator_proposals,
    header_json :validator_reward :: FLOAT AS validator_reward,
    blocks_final_id AS fact_blocks_id,
    inserted_timestamp,
    modified_timestamp
FROM
    blocks
)

SELECT * FROM __dbt__cte__core__fact_blocks
)

SELECT
    block_id,
    block_timestamp,
    block_hash,
    block_author,
    header,
    block_challenges_result,
    block_challenges_root,
    chunk_headers_root,
    chunk_tx_root,
    chunk_mask,
    chunk_receipts_root,
    chunks,
    chunks_included,
    epoch_id,
    epoch_sync_data_hash,
    gas_price,
    last_ds_final_block,
    last_final_block,
    latest_protocol_version,
    next_bp_hash,
    next_epoch_id,
    outcome_root,
    prev_hash,
    prev_height,
    prev_state_root,
    random_value,
    rent_paid,
    signature,
    total_supply,
    validator_proposals,
    validator_reward,
    fact_blocks_id,
    inserted_timestamp,
    modified_timestamp
FROM live_blocks_call
WHERE
    -- Filter based on block numbers greater than the max in the gold table.
    -- This might be slightly redundant given the UDTF call starts at max_block_id + 1,
    -- but ensures correctness if the UDTF were to behave unexpectedly.
    block_id > (SELECT max_block_id FROM max_gold_block)