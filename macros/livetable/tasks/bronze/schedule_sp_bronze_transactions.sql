{% macro create_fact_tx_sproc_task()%}

{% set task_query %}
	CREATE TASK IF NOT EXISTS {{ target.database }}.LIVETABLE.TASK_REFRESH_FACT_TRANSACTION_LIVE_TEST
		WAREHOUSE=DATA_PLATFORM_DEV
		SCHEDULE='4 MINUTE'
		QUERY_TAG='{"project": "near_models", "model": "sp_refresh_fact_transactions_live_test", "model_type": "core"}'
		AS CALL {{ target.database }}.LIVETABLE.SP_REFRESH_FACT_TRANSACTIONS_LIVE();
{% endset %}

{% if execute %}
    {% do run_query(task_query) %}
    {% do log("Deployed task TASK_UPDATE_RECENT_HYBRID_TX", info=True) %}
	ALTER TASK {{ target.database }}.LIVETABLE.TASK_REFRESH_FACT_TRANSACTION_LIVE_TEST RESUME;
	{% do log("Updated task state to resumed for TASK_UPDATE_RECENT_HYBRID_TX", info=True) %}
{% endif %}
{% endmacro %}

