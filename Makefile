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
deploy_live_table_udtfs: rm_logs
	dbt run \
	-s near_models.deploy.near.live__table \
	--vars '{UPDATE_UDFS_AND_SPS: true, ENABLE_LIVE_TABLE: true, LIVE_TABLE_MATERIALIZATION: ephemeral}' \
	--profiles-dir ~/.dbt \
	--profile near \
	--target dev
