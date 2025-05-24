{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'omni_address',
    tags = ['scheduled_non_core']
) }}

WITH omni AS (

    SELECT
        VALUE:CONTRACT_ADDRESS::STRING AS omni_address,
        DATA
    FROM
        {{ ref('bronze__omni_metadata')}}
    WHERE
        typeof(DATA) != 'NULL_VALUE'

    {% if is_incremental() %}
    AND
        _inserted_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
),
flattened AS (
    SELECT
        omni_address,
        value::INT AS byte_val,
        index
    FROM 
        omni,
        LATERAL FLATTEN(input => DATA:result)
),
hex_strings AS (
    SELECT
        omni_address,
        LISTAGG(LPAD(TO_CHAR(byte_val, 'XX'), 2, '0'), '') WITHIN GROUP (ORDER BY index) AS hex_string
    FROM 
        flattened
    GROUP BY 
        omni_address
),
decode AS (
    SELECT
        omni_address,
        TRY_PARSE_JSON(
            livequery.utils.udf_hex_to_string(hex_string)
        ) AS decoded_result
    FROM 
        hex_strings
)
SELECT
    omni_address,
    decoded_result AS contract_address,
    {{ dbt_utils.generate_surrogate_key(
        ['omni_address']
    ) }} AS omni_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    decode
qualify(row_number() over (partition by omni_address order by inserted_timestamp asc)) = 1
