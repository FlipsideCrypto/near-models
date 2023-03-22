{% macro get_s3_filenames() %}
  {% set create_table %}
  CREATE schema if NOT EXISTS {{ target.database }}.bronze;
CREATE TABLE if NOT EXISTS {{ target.database }}.bronze.s3_filenames(
    NAME VARCHAR,
    SIZE NUMBER,
    MD5 VARCHAR,
    last_modified VARCHAR
  );
{% endset %}
  {% do run_query(create_table) %}
  {% set get_block %}
SELECT
  LPAD(
    COALESCE(MAX(SPLIT(NAME, '/') [3] :: NUMBER), 9820210) :: STRING,
    12,
    '0'
  ) AS block_id,
  FLOOR(
    COALESCE(MAX(SPLIT(NAME, '/') [3] :: NUMBER), 9820210),
    -4
  ) AS _partition_by_block_number
FROM
  {{ target.database }}.bronze.s3_filenames;
{% endset %}
  {% if execute %}
    {% set max_block_id_indexed = run_query(get_block).columns [0].values() [0] %}
  {% endif %}

  {% set block_range_start_prefix = max_block_id_indexed %}
  {% set load_filenames %}
  list @STREAMLINE.BRONZE.STG_US_EAST_1_SERVERLESS_NEAR_LAKE_MAINNET_FSC/{{ block_range_start_prefix [:-5] }};
MERGE INTO {{ target.database }}.bronze.s3_filenames tgt USING (
    SELECT
      *
    FROM
      TABLE(RESULT_SCAN(LAST_QUERY_ID()))) src
      ON tgt.name = src."name"
      WHEN NOT matched THEN
    INSERT
      (
        NAME,
        SIZE,
        MD5,
        last_modified
      )
    VALUES
      (
        src."name",
        src."size",
        src."md5",
        src."last_modified"
      );
{% endset %}
      {% do run_query(load_filenames) %}
{% endmacro %}
