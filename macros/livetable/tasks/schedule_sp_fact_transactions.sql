{% macro create_fact_tx_sproc_task()%}
CREATE TASK IF NOT EXISTS {{ target.database }}.LIVETABLE.TASK_UPDATE_RECENT_HYBRID_TX
	WAREHOUSE=DBT_CLOUD
	SCHEDULE='5 MINUTE'
	QUERY_TAG='sp_near_live_fact_transactions' -- TODO: Use repo specific standard for tags JSON
	AS CALL {{ target.database }}.LIVETABLE.SP_REFRESH_FACT_TRANSACTIONS_LIVE();
{% endmacro %}

