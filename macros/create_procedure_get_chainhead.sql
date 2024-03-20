{% macro create_PROCEDURE_GET_CHAINHEAD() %}
    {% set sql %}
        CREATE OR REPLACE PROCEDURE {{ target.database }}.STREAMLINE.GET_CHAINHEAD(
        ) RETURNS STRING
        LANGUAGE JAVASCRIPT
        EXECUTE AS CALLER
        AS
        $$
        let resultFound = false;
        let blockId = 0;
        while (!resultFound) {
            var stmt = snowflake.createStatement({
                sqlText: `SELECT {{ target.database }}.live.udf_api(
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
                ) :data :result :sync_info :latest_block_height :: INT AS block_id;`
            });
            var result = stmt.execute();
            result.next();
            res_temp = result.getColumnValue(1); 
            if (res_temp != null) {
                res = res_temp;
                resultFound = true;
            }
        }
        return res;
        $$;
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
