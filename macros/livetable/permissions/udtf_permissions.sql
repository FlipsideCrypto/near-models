{% macro lt_udtf_permissions() %}
     

    {% set udtfs = [
        '{{ target.database }}.livetable.lt_blocks_udf_api',
        '{{ target.database }}.livetable.lt_tx_udf_api',
        '{{ target.database }}.livetable.lt_chunks_udf_api',
    ] %}

    {% for udtf in udtfs %}
        GRANT USAGE ON FUNCTION {{ udtf }} TO ROLE {{ target.role }};
    {% endfor %}

    {% return udtf_permissions %}