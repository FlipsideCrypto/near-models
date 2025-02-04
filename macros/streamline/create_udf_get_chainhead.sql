-- todo, what if we receive a bad response?
-- ask for backend deployment?
{% macro create_UDF_GET_CHAINHEAD() %}
    {% set sql %}
        CREATE
        OR REPLACE FUNCTION {{ target.database }}.STREAMLINE.UDF_GET_CHAINHEAD(
        ) returns INTEGER 
        AS $$ 
            SELECT
                {{ target.database }}.live.udf_api(
                    'POST',
                    '{Service}',
                    OBJECT_CONSTRUCT(
                        'Content-Type', 'application/json'
                    ),
                    OBJECT_CONSTRUCT(
                        'jsonrpc', '2.0',
                        'id', 'Flipside/getChainhead/0.1',
                        'method', 'status',
                        'params', OBJECT_CONSTRUCT(
                            'finality', 'final' 
                        )
                    ),
                    'Vault/prod/near/quicknode/mainnet'
            ) :data :result :sync_info :latest_block_height :: INT AS block_id
        
        $$ 
     {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
