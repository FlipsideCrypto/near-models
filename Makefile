SHELL := /bin/bash

dbt-console: 
	docker-compose run dbt_console

.PHONY: dbt-console

decoder_poc: 
	dbt run \
	--vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' \
	-m 1+models/streamline/poc/decoder/streamline__decoded_input_events.sql \
	--profile near \
	--target dev \
	--profiles-dir ~/.dbt

rm_logs:
	@if [ -d logs ]; then \
		rm -r logs 2>/dev/null || echo "Warning: Could not remove logs directory"; \
	else \
		echo "Logs directory does not exist"; \
	fi

# deploy live table udtfs
deploy_livetable_udtfs: rm_logs
	dbt run \
	-s near_models.deploy.livetable \
	--vars '{UPDATE_UDFS_AND_SPS: true, ENABLE_LIVE_TABLE: true, LIVE_TABLE_MATERIALIZATION: ephemeral}' \
	--profiles-dir ~/.dbt \
	--profile near \
	--target dev

deploy_tx_sproc: rm_logs
	dbt run-operation create_sp_refresh_fact_transactions_live \
	--profiles-dir ~/.dbt \
	--profile near \
	--target dev

deploy_tx_task: rm_logs
	dbt run-operation create_fact_tx_sproc_task \
	--profiles-dir ~/.dbt \
	--profile near \
	--target dev

compile_sp_macro: rm_logs
	dbt compile --select _compile_sp_macro \
	--profiles-dir ~/.dbt \
	--profile near \
	--target dev

compile_task: rm_logs
	dbt compile --select _compile_task \
	--profiles-dir ~/.dbt \
	--profile near \
	--target dev

test_deploy_sf_tasks: rm_logs
	dbt run --select _dummy_model \
	--profiles-dir ~/.dbt \
	--profile near \
	--target dev

