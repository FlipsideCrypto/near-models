{% macro create_UDF_GET_CHAINHEAD() %}
    {% set sql %}
    {#
    EXECUTE A method
    ON A deployed near smart contract USING THE `finality` block PARAMETER BY DEFAULT.signature STRING,
    STRING,
    OBJECT #}
    CREATE
    OR REPLACE FUNCTION {{ target.database }}.CORE.UDF_GET_CHAINHEAD(
    ) returns INTEGER 
    AS $$ 
        SELECT
            ethereum.streamline.udf_api(
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
                        'finality': 'final' }
                }
        ) :data :result :sync_info :latest_block_height :: INT AS block_id
    
     $$ 
     {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
