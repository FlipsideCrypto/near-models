{% macro create_get_nearblocks_fts() %}

  {% set create_table %}
  CREATE schema if NOT EXISTS bronze_api;
CREATE TABLE if NOT EXISTS bronze_api.nearblocks_fts(
    loop_num INTEGER,
    token_name STRING,
    token_contract STRING,
    token_data variant,
    provider STRING,
    _inserted_timestamp timestamp_ntz,
    _res_id STRING
  );
{% endset %}
  {% do run_query(create_table) %}

{% set query %}
CREATE OR REPLACE PROCEDURE {{ target.database }}.bronze_api.get_nearblocks_fts() returns variant language sql as $$
BEGIN
  LET counter := 0;
  WHILE (counter < 15) DO
  
  LET page := counter;
  
create or replace temporary table response_data as 
  WITH api_call AS (
    SELECT
    ethereum.streamline.udf_api(
        'GET', 
        'https://api.nearblocks.io/v1/fts', 
        {}, 
        {
            'page': :page,
            'per_page': 50,
            'sort': 'name',
            'order': 'asc'
        }
      ) AS res,
      ARRAY_SIZE(
        res :data :tokens
      ) AS token_count,
      CURRENT_TIMESTAMP AS _request_timestamp
  ),
  flatten_res AS (
    SELECT
      VALUE :name :: STRING AS token_name,
      VALUE :contract :: STRING AS token_contract,
      VALUE AS token_data,
      'nearblocks' AS provider,
      _request_timestamp AS _inserted_timestamp,
      concat_ws('-', DATE_PART(epoch_second, _request_timestamp), token_contract) AS _res_id
    FROM
      api_call,
      LATERAL FLATTEN(
        input => res :data :tokens
      )
  )
SELECT
  token_name,
  token_contract,
  token_data,
  provider,
  _inserted_timestamp,
  _res_id
FROM
  flatten_res;


  INSERT INTO
  bronze_api.nearblocks_fts(
    loop_num,
    token_name,
    token_contract,
    token_data,
    provider,
    _inserted_timestamp,
    _res_id
  )
SELECT
:page as loop_num,
  token_name,
  token_contract,
  token_data,
  provider,
  _inserted_timestamp,
  _res_id
FROM
  response_data;

    counter := counter + 1;
  END WHILE;
  RETURN counter;
END;$$

{% endset %}
{% do run_query(query) %}

{% endmacro %}
