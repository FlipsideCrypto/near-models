{% macro create_UDF_GET_CHAINHEAD() %}
    {% set sql %}
        CREATE
        OR REPLACE FUNCTION {{ target.database }}.STREAMLINE.UDF_GET_CHAINHEAD(
        ) returns INTEGER 
        AS $$ 
            SELECT
                {{ target.database }}.live.udf_api(
                    'POST',
                    'https://rpc.mainnet.near.org',
                    { 
                        'Content-Type': 'application/json' 
                    },
                    { 
                        'jsonrpc': '2.0',
                        'id': 'dontcare',
                        'method' :'status',
                        'params':{
                            'finality': 'final' 
                            }
                    }
            ) :data :result :sync_info :latest_block_height :: INT AS block_id
        
        $$ 
     {% endset %}
    {% do run_query(sql) %}
{% endmacro %}