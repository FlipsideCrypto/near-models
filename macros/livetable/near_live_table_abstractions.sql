-- Get Near Chain Head

{% macro near_live_table_latest_block_height() %}
WITH rpc_call AS (
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
                _utils.UDF_WHOAMI(),
                'Vault/prod/near/quicknode/livetable/mainnet'
        ):data::object AS rpc_result
)
SELECT
    rpc_result:result:header:height::INTEGER AS latest_block_height
FROM
    rpc_call
{% endmacro %}

{% macro near_live_table_min_max_block_height(start_block, block_count) %}
SELECT
    {{ start_block }} AS min_height,
    min_height + {{ block_count }} AS max_height, 
FROM
    dual    
{% endmacro %}

-- Get Near Block Data
{% macro near_live_table_target_blocks(start_block, block_count) %}
    
    WITH heights AS (
        SELECT
            min_height,
            max_height,
        FROM (
             {{- near_live_table_min_max_block_height(start_block=start_block, block_count=block_count) | indent(13) -}}
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
            TABLE(generator(ROWCOUNT => {{ block_count }} )) 
        qualify block_number BETWEEN h.min_height AND h.max_height
    )
    SELECT
        block_number as block_height    
    FROM block_spine
{% endmacro %}

{% macro near_live_table_get_spine(table_name) %}
SELECT
    block_height,
    ROW_NUMBER() OVER (ORDER BY block_height) - 1 as partition_num
FROM 
    (
        SELECT 
            row_number() over (order by seq4()) - 1 + COALESCE(block_id, 0)::integer as block_height,
            min_height,
            max_height
        
        FROM
                TABLE(generator(ROWCOUNT => IFF(
                    COALESCE(to_latest, false),
                    latest_block_height - min_height + 1,
                    1
                ))),
                {{ table_name }}
            qualify block_height BETWEEN min_height AND max_height
    )
{% endmacro %}
   
{% macro near_live_table_get_raw_block_data(spine) %} 
SELECT
    block_height,
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS request_timestamp,
    livetable.lt_blocks_udf_api(
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
        'Vault/prod/near/quicknode/livetable/mainnet'
    ):data.result AS rpc_data_result
from
    {{spine}}

{% endmacro %}

{% macro near_live_table_extract_raw_block_data(raw_blocks) %}
SELECT 
    block_data:header:height::string as block_id,
    TO_TIMESTAMP_NTZ(
            block_data :header :timestamp :: STRING
        ) AS block_timestamp, 
    block_data:header:hash::STRING as block_hash,
    ARRAY_SIZE(block_data:chunks)::NUMBER as tx_count,
    block_data:header as header,
    block_data:header:challenges_result::ARRAY as block_challenges_result,
    block_data:header:challenges_root::STRING as block_challenges_root,
    block_data:header:chunk_headers_root::STRING as chunk_headers_root,
    block_data:header:chunk_tx_root::STRING as chunk_tx_root,
    block_data:header:chunk_mask::ARRAY as chunk_mask,
    block_data:header:chunk_receipts_root::STRING as chunk_receipts_root,
    block_data:chunks as chunks,
    block_data:header:chunks_included::NUMBER as chunks_included,
    block_data:header:epoch_id::STRING as epoch_id,
    block_data:header:epoch_sync_data_hash::STRING as epoch_sync_data_hash,
    block_data:events as events,
    block_data:header:gas_price::NUMBER as gas_price,
    block_data:header:last_ds_final_block::STRING as last_ds_final_block,
    block_data:header:last_final_block::STRING as last_final_block,
    block_data:header:latest_protocol_version::NUMBER as latest_protocol_version,
    block_data:header:next_bp_hash::STRING as next_bp_hash,
    block_data:header:next_epoch_id::STRING as next_epoch_id,
    block_data:header:outcome_root::STRING as outcome_root,
    block_data:header:prev_hash::STRING as prev_hash,
    block_data:header:prev_height::NUMBER as prev_height,
    block_data:header:prev_state_root::STRING as prev_state_root,
    block_data:header:random_value::STRING as random_value,
    block_data:header:rent_paid::FLOAT as rent_paid,
    block_data:header:signature::STRING as signature,
    block_data:header:total_supply::NUMBER as total_supply,
    block_data:header:validator_proposals as validator_proposals,
    block_data:header:validator_reward::NUMBER as validator_reward,
    MD5(block_data:header:height::STRING) as fact_blocks_id,
    SYSDATE() as inserted_timestamp,
    SYSDATE() as modified_timestamp
FROM {{raw_blocks}}

{% endmacro %}

{% macro near_live_table_fact_blocks(schema, blockchain, network) %}
    {%- set near_live_table_fact_blocks = livequery_models.get_rendered_model('near_models', 'livetable_fact_blocks', schema, blockchain, network) -%}
    {{ near_live_table_fact_blocks }}
{% endmacro %}

{% macro near_live_table_fact_transactions(schema, blockchain, network) %}
    {%- set near_live_table_fact_transactions = livequery_models.get_rendered_model('near_models', 'livetable_fact_transactions', schema, blockchain, network) -%}
    {{ near_live_table_fact_transactions }}
{% endmacro %}

{% macro near_live_table_fact_receipts(schema, blockchain, network) %}
    {%- set near_live_table_fact_receipts = livequery_models.get_rendered_model('near_models', 'livetable_fact_receipts', schema, blockchain, network) -%}
    {{ near_live_table_fact_receipts }}
{% endmacro %}

{% macro near_live_table_ez_actions(schema, blockchain, network) %}
    {%- set near_live_table_ez_actions = livequery_models.get_rendered_model('near_models', 'livetable_ez_actions', schema, blockchain, network) -%}
    {{ near_live_table_ez_actions }}
{% endmacro %}
