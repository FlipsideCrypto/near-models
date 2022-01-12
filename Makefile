SHELL := /bin/bash

dbt-console: 
	docker-compose run dbt_console

dbt-docs: 
	docker-compose run --service-ports dbt_docs 

.PHONY: dbt-console