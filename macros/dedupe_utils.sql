{% macro deduped_blocks(table_name) -%}
    (
        select 
            *
        from {{ source("chainwalkers", table_name) }}
        qualify row_number() over (partition by block_id order by ingested_at desc) = 1
    )
{%- endmacro %}

{% macro deduped_txs(table_name ) -%}
    (
        select 
            *
        from {{ source("chainwalkers", table_name) }}
        qualify row_number() over (partition by tx_id order by ingested_at desc) = 1
    )
{%- endmacro %}
