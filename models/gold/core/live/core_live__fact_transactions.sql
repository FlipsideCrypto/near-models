
{{ config(
    materialized='dynamic_table',
    refresh_mode="auto",
    target_lag='1 minute',
    snowflake_warehouse='DBT_CLOUD', 
    query_tag = 'near_fact_transactions_live',
    transient=false        
) }}


WITH max_gold_block AS (
    
    SELECT
        COALESCE(MAX(block_id), 0) AS max_block_id
    FROM {{ ref('core__fact_transactions') }}
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

live_transactions_call AS (
    WITH __dbt__cte__bronze__transactions AS (
        -- LIVE LOGIC: Call RPCs to populate live table
        SELECT 1
    ),

    __dbt__cte__bronze__FR_transactions AS (
        WITH spine AS (
            
            
            WITH heights AS (
                SELECT
                    min_height,
                    max_height,
                FROM (
                    SELECT
                        _block_height AS min_height,
                        min_height + row_count AS max_height, 
                    FROM
                        dual    
        )
            ),
            block_spine AS (
                SELECT
                    ROW_NUMBER() OVER (
                        ORDER BY
                            NULL
                    ) - 1 + h.min_height::integer AS block_number,
                FROM
                    heights h, 
                    TABLE(generator(ROWCOUNT => row_count )) 
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
                _utils.UDF_WHOAMI(),
                'Vault/prod/near/quicknode/mainnet'
            ):data.result AS rpc_data_result
        from
            spine

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
    ),

    __dbt__cte__silver__transactions_v2 AS (
        -- depends_on: __dbt__cte__bronze__transactions
    -- depends_on: __dbt__cte__bronze__FR_transactions


    WITH bronze_transactions AS (

        SELECT
            VALUE :BLOCK_ID :: INT AS origin_block_id,
            VALUE :BLOCK_TIMESTAMP_EPOCH :: INT AS origin_block_timestamp_epoch,
            VALUE :SHARD_ID :: INT AS shard_id,
            VALUE :CHUNK_HASH :: STRING AS chunk_hash,
            VALUE :HEIGHT_CREATED :: INT AS chunk_height_created,
            VALUE :HEIGHT_INCLUDED :: INT AS chunk_height_included,
            DATA :transaction :hash :: STRING AS tx_hash,
            DATA :transaction :signer_id :: STRING AS signer_id,
            partition_key,
            DATA :: variant AS response_json,
            _inserted_timestamp
        FROM


                __dbt__cte__bronze__FR_transactions
            WHERE
                typeof(DATA) != 'NULL_VALUE'
            
        )
    SELECT
        origin_block_id,
        origin_block_timestamp_epoch,
        TO_TIMESTAMP_NTZ(origin_block_timestamp_epoch, 9) AS origin_block_timestamp,
        shard_id,
        chunk_hash,
        chunk_height_created,
        chunk_height_included,
        tx_hash,
        signer_id,
        partition_key,
        response_json,
        _inserted_timestamp,
        
        
    md5(cast(coalesce(cast(tx_hash as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS transactions_v2_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        'aefe7ba3-d8d8-471c-8781-63edf82cf1b8' AS _invocation_id
    FROM
        bronze_transactions 

    qualify ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
    ),

    __dbt__cte__silver__transactions_final AS (
        WITH txs_with_receipts AS (
    SELECT
        chunk_hash,
        origin_block_id AS block_id,
        origin_block_timestamp AS block_timestamp,
        tx_hash,
        response_json :transaction :: variant AS transaction_json,
        response_json :transaction_outcome :outcome :: variant AS outcome_json,
        response_json :status :: variant AS status_json,
        response_json :receipts_outcome :: ARRAY AS receipts_outcome_json,
        response_json :status :Failure IS NULL AS tx_succeeded,
        partition_key AS _partition_by_block_number
    FROM
        __dbt__cte__silver__transactions_v2

        

    ),
    determine_receipt_gas_burnt AS (
    SELECT
        tx_hash,
        SUM(
        ZEROIFNULL(VALUE :outcome :gas_burnt :: INT)
        ) AS total_gas_burnt_receipts,
        SUM(
        ZEROIFNULL(VALUE :outcome :tokens_burnt :: INT)
        ) AS total_tokens_burnt_receipts
    FROM
        txs_with_receipts,
        LATERAL FLATTEN (
        input => receipts_outcome_json
        )
    GROUP BY
        1
    ),
    determine_attached_gas AS (
    SELECT
        tx_hash,
        SUM(
        VALUE :FunctionCall :gas :: INT
        ) AS total_attached_gas
    FROM
        txs_with_receipts,
        LATERAL FLATTEN (
        input => transaction_json :actions :: ARRAY
        )
    GROUP BY
        1
    ),
    transactions_final AS (
    SELECT
        chunk_hash,
        block_id,
        block_timestamp,
        t.tx_hash,
        transaction_json,
        outcome_json,
        status_json,
        total_gas_burnt_receipts,
        total_tokens_burnt_receipts,
        total_attached_gas,
        tx_succeeded,
        _partition_by_block_number
    FROM
        txs_with_receipts t
        LEFT JOIN determine_receipt_gas_burnt d USING (tx_hash)
        LEFT JOIN determine_attached_gas A USING (tx_hash)
    )
    SELECT
    chunk_hash,
    block_id,
    block_timestamp,
    tx_hash,
    transaction_json :receiver_id :: STRING AS tx_receiver,
    transaction_json :signer_id :: STRING AS tx_signer,
    transaction_json,
    outcome_json,
    status_json,
    tx_succeeded,
    ZEROIFNULL(outcome_json :gas_burnt :: INT) + total_gas_burnt_receipts AS gas_used,
    ZEROIFNULL(outcome_json :tokens_burnt :: INT) + total_tokens_burnt_receipts AS transaction_fee,
    COALESCE(
        total_attached_gas,
        gas_used
    ) AS attached_gas,
    _partition_by_block_number,
    
        
    md5(cast(coalesce(cast(tx_hash as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS transactions_final_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    'aefe7ba3-d8d8-471c-8781-63edf82cf1b8' AS _invocation_id
    FROM
    transactions_final
    ),

    __dbt__cte__core__fact_transactions AS (
        SELECT
        tx_hash,
        block_id,
        block_timestamp,
        transaction_json :nonce :: INT AS nonce,
        transaction_json :signature :: STRING AS signature,
        tx_receiver,
        tx_signer,
        transaction_json AS tx,
        gas_used,
        transaction_fee,
        attached_gas,
        tx_succeeded,
        transactions_final_id AS fact_transactions_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        __dbt__cte__silver__transactions_final
    )
    SELECT * FROM __dbt__cte__core__fact_transactions
)
SELECT
    ltc.tx_hash,
    ltc.block_id,
    ltc.block_timestamp,
    ltc.nonce,
    ltc.signature,
    ltc.tx_receiver,
    ltc.tx_signer,
    ltc.tx, -- contains the transaction_json
    ltc.gas_used,
    ltc.transaction_fee,
    ltc.attached_gas,
    ltc.tx_succeeded,
    ltc.fact_transactions_id,
    ltc.inserted_timestamp,
    ltc.modified_timestamp
FROM live_transactions_call ltc
WHERE
    block_id > (SELECT max_block_id FROM max_gold_block)