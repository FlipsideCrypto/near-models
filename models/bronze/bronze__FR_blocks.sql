{{ config (
    materialized = var('LIVE_TABLE_MATERIALIZATION', 'view'),
    tags = ['streamline_helper']
) }}

{% if var('ENABLE_LIVE_TABLE', false) %}
    
    {%- set blockchain = this.schema -%}
    {%- set network = this.identifier -%}
    {%- set schema = blockchain ~ "_" ~ network -%}

    WITH spine AS (
        {{ near_livetable_target_blocks(start_block='_block_height', block_count='row_count') | indent(4) -}}
    ),
    raw_blocks AS (
        {{ near_livetable_get_raw_block_data('spine') | indent(4) -}}
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
{% else %}
    -- BATCH LOGIC: Default
    {{ streamline_external_table_FR_query_v2(
        model = "blocks_v2",
        partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER )") 
    }}
{% endif %}


