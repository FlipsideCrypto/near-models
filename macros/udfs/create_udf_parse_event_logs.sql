{% macro create_udf_parse_event_logs() %}
{% set sql %}
CREATE OR REPLACE FUNCTION {{ target.database }}.STREAMLINE.UDF_PARSE_EVENT_LOGS(logs ARRAY)
RETURNS ARRAY
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'parse_event_logs'
AS
$$
import json
def parse_event_logs(logs: list) -> list:
    return [
        json.loads(log[len("EVENT_JSON:"):]) if log.startswith("EVENT_JSON:") and is_valid_json(log[len("EVENT_JSON:"):])
        else log
        for log in logs
    ]

def is_valid_json(json_str: str) -> bool:
    try:
        json.loads(json_str)
        return True
    except json.JSONDecodeError:
        return False
$$;
{% endset %}
{% do run_query(sql) %}
{% endmacro %}
