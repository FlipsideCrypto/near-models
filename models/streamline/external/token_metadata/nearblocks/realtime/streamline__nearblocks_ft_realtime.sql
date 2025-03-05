-- depends_on: {{ ref('streamline__ft_tokenlist') }}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "nearblocks_ft_metadata",
            "sql_limit": "1",
            "producer_batch_size": "1",
            "worker_batch_size": "1",
            "sql_source": "{{this.identifier}}"
        }
    ),
    tags = ['streamline_non_core']
) }}

WITH
ft_tokenlist AS (

    SELECT
        contract_address
    FROM
        {{ ref('streamline__ft_tokenlist') }}
    EXCEPT
    SELECT
        contract_address
    FROM
        {{ ref('streamline__nearblocks_ft_complete')}}
)
SELECT
    contract_address,
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        '{Service}fts/' || contract_address,
        {},
        {},
        'Vault/prod/near/nearblocks'
    ) AS request
FROM
    ft_tokenlist
