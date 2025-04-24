{% macro create_fact_tx_sproc_task()%}
CREATE OR REPLACE TASK near_dev.live_table.task_update_recent_hybrid_tx
WAREHOUSE = DBT_CLOUD 
SCHEDULE = '5 MINUTE' 
ALLOW_OVERLAPPING_EXECUTION = TRUE 
AS
    BEGIN
        ALTER SESSION SET QUERY_TAG = 'sp_near_live_fact_transactions';
        CALL sp_update_recent_hybrid_transactions();
    END;
{% endmacro %}

