{{ config (
    materialized = var('LIVE_TABLE_MATERIALIZATION', 'view'),
    tags = ['streamline_helper']

) }}

{% if var('ENABLE_LIVE_TABLE_QUERY', false) %}
    -- LIVE LOGIC: Call RPCs to populate live table
    SELECT 1
{% else %}
    -- BATCH LOGIC: Default
    {{ streamline_external_table_query_v2(
        model = "transactions_v2",
        partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER )"
    ) }}
{% endif %}


