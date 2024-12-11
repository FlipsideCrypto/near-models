{{ config (
    materialized = 'view',
    tags = ['load', 'load_shards','scheduled_core']
) }}

{{ streamline_external_table_FR_query_v2(
    model = "shards",
    partition_function = "(concat(left(split_part(file_name, '/', 1), 9), '000')::integer)"
) }}
