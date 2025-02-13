{{ config (
    materialized = 'view',
    tags = ['streamline_helper']
) }}

{{ streamline_external_table_FR_query_v2(
    model = "chunks_v2",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER )"
) }}
