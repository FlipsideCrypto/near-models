{# list @STG_US_EAST_1_SERVERLESS_NEAR_LAKE_MAINNET_FSC/ #}
{% macro create_get_s3_filenames() %}
  {% set create_table %}
  CREATE schema if NOT EXISTS {{ target.database }}.bronze_api;
CREATE TABLE if NOT EXISTS {{ target.database }}.bronze_api.s3_filenames(
    NAME VARCHAR,
    SIZE NUMBER,
    MD5 VARCHAR,
    last_modified VARCHAR
  );
{% endset %}
  {% do run_query(create_table) %}
  {# this works to query the external table and list filenames #}
  {# todo - add pagination from last max block id/partition
    a backfill is not exactly necessary, but it could be nice to have
   #}
  {% set query %}
  list @STREAMLINE.BRONZE.STG_US_EAST_1_SERVERLESS_NEAR_LAKE_MAINNET_FSC/00008775;
INSERT INTO
  {{ target.database }}.bronze_api.s3_filenames
SELECT
  *
FROM
  TABLE(RESULT_SCAN(LAST_QUERY_ID()));
{% endset %}
  {% do run_query(query) %}
{% endmacro %}
