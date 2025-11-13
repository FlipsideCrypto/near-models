{{ config(
    materialized = 'incremental',
    unique_key = 'chainlist_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core']
) }}

WITH api_call AS (

    SELECT
        response
    FROM
        {{ ref('streamline__chainlist_ids_realtime') }}
),
flattened AS (
    SELECT
        VALUE :chain ::STRING AS chain,
        VALUE :chainId :: INT AS chain_id,
        VALUE :name :: STRING AS chain_name
    FROM
        api_call,
        LATERAL FLATTEN(
            input => response :data :: ARRAY
        )
)
SELECT
    chain,
    chain_id,
    chain_name,
    {{ dbt_utils.generate_surrogate_key(['chain_id']) }} AS chainlist_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flattened

qualify(row_number() over (partition by chain_id order by inserted_timestamp asc)) = 1
