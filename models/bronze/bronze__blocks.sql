{{ config (
    materialized = 'view',
    tags = ['load', 'load_blocks','scheduled_core']
) }}

{{ streamline_external_table_query_v2(
    model = "blocks",
    partition_function = "(concat(left(split_part(file_name, '/', 1), 9), '000')::integer)"
) }}
