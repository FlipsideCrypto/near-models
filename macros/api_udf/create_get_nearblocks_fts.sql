{% macro create_get_nearblocks_fts() %}
  {% set create_table %}
  CREATE schema if NOT EXISTS {{ target.database }}.bronze_api;
CREATE TABLE if NOT EXISTS {{ target.database }}.bronze_api.nearblocks_fts(
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
  {% set max_loops = 20 %}
  CREATE
  OR REPLACE PROCEDURE {{ target.database }}.bronze_api.get_nearblocks_fts() returns variant LANGUAGE SQL AS $$
BEGIN
  let counter:= 1;
  let number_of_iterations:= 0;
REPEAT 
number_of_iterations:= number_of_iterations + 1;
let page:= counter;
CREATE
  OR REPLACE temporary TABLE response_data AS WITH api_call AS (
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
    token_name,
    token_contract,
    token_data,
    provider,
    _inserted_timestamp,
    _res_id
  )
SELECT
  token_name,
  token_contract,
  token_data,
  provider,
  _inserted_timestamp,
  _res_id
FROM
  response_data;
counter:= counter + 1;
until (
    counter = {{ max_loops }}
    OR (
      SELECT
        COUNT(1) = 0
      FROM
        response_data
    )
  )
END REPEAT;
RETURN number_of_iterations;
END;$$ 
{% endset %}
{% do run_query(query) %}
{% endmacro %}
