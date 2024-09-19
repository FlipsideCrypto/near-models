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