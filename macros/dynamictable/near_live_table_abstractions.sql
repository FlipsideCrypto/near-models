
{% macro near_dynamic_table_fact_blocks(schema, blockchain, network, start_block, block_count) %}

    {%- set _block_height = start_block -%}
    {%- set row_count = block_count -%}

    {%- set near_dynamic_table_fact_blocks = livequery_models.get_rendered_model('near_models', 'livetable_fact_blocks', schema, blockchain, network) -%}
    {{ near_dynamic_table_fact_blocks }}
{% endmacro %}



