
    {{ config(
        materialized='dynamic_table',
        target_lag='1 minute',          
        warehouse='DBT_CLOUD',  
        transient=false        
    ) }}

    WITH max_gold_block AS (
        
        SELECT
            COALESCE(MAX(block_id), 0) AS max_block_id
        FROM {{ ref('silver__blocks_final') }}
    ),
    chain_head AS (
        SELECT 
            near_dev.live_table.udf_get_latest_block_height() AS latest_block_id
    ),
    fetch_parameters AS (
        SELECT
            mgb.max_block_id + 1 AS start_block_id,
            -- TODO: Replace with GREATEST()
            LEAST(ch.latest_block_id - mgb.max_block_id, 10)::INTEGER AS num_rows_to_fetch
        FROM max_gold_block mgb, chain_head ch
    ),
    live_blocks_call AS (
        SELECT
            tf.*
        FROM
            fetch_parameters fp,
            TABLE(live_table.tf_fact_blocks(
                    fp.start_block_id,
                    fp.num_rows_to_fetch
                )) AS tf
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