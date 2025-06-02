{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'omni_asset_identifier',
    tags = ['scheduled_non_core']
) }}

WITH omni AS (

    SELECT
        VALUE:CONTRACT_ADDRESS::STRING AS omni_asset_identifier,
        VALUE:data:result:result::ARRAY AS result_array,
        DATA
    FROM
        {{ ref('bronze__omni_metadata') }}
    {% if is_incremental() %}
    WHERE
        _inserted_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
),
try_decode_hex AS (
    SELECT 
        omni_asset_identifier,
        b.value AS raw,
        b.index,
        LPAD(TRIM(to_char(b.value::INT, 'XXXXXXX'))::STRING, 2, '0') AS hex,
        ROW_NUMBER() OVER (PARTITION BY omni_asset_identifier, raw, index, hex ORDER BY omni_asset_identifier) AS rn
    FROM omni o,
    TABLE(FLATTEN(o.result_array, recursive => TRUE)) b
    WHERE IS_ARRAY(o.result_array) = TRUE
    ORDER BY 1, 3
),
decoded_response AS (
    SELECT 
        omni_asset_identifier,
        ARRAY_TO_STRING(ARRAY_AGG(hex) WITHIN GROUP (ORDER BY index ASC), '') AS decoded_response
    FROM try_decode_hex
    GROUP BY omni_asset_identifier, rn
    HAVING rn = 1
),
conversion AS (
    SELECT
        omni_asset_identifier,
        TRIM(livequery.utils.udf_hex_to_string(decoded_response), '"') AS decoded_result
    FROM decoded_response
)
SELECT
    omni_asset_identifier,
    decoded_result AS contract_address,
    {{ dbt_utils.generate_surrogate_key(
        ['omni_asset_identifier']
    ) }} AS omni_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    conversion
qualify(row_number() over (partition by omni_asset_identifier order by inserted_timestamp asc)) = 1
