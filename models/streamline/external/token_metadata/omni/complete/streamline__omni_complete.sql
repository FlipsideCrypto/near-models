-- depends_on: {{ ref('bronze__omni_metadata') }}
{{ config(
    materialized = "incremental",
    unique_key = "contract_address",
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['streamline_non_core']
) }}

SELECT
    

