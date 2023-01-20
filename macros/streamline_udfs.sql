{% macro create_udf_introspect() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_introspect(
        echo STRING
    ) returns text api_integration = aws_stg_us_east_1_api max_batch_rows = 10 AS {% if target.name == "prod" %}
        'https://jfqhk99kj1.execute-api.us-east-1.amazonaws.com/stg/s3/introspect'
    {% else %}
        'https://jfqhk99kj1.execute-api.us-east-1.amazonaws.com/stg/s3/introspect'
    {%- endif %};
{% endmacro %}

{% macro create_udf_s3_list_directories() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_s3_list_directories(
        path STRING
    ) returns ARRAY api_integration = aws_stg_us_east_1_api max_batch_rows = 10 AS {% if target.name == "prod" %}
        'https://jfqhk99kj1.execute-api.us-east-1.amazonaws.com/stg/s3/list_directories'
    {% else %}
        'https://jfqhk99kj1.execute-api.us-east-1.amazonaws.com/stg/s3/list_directories'
    {%- endif %};
{% endmacro %}

{% macro create_udf_s3_list_objects() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_s3_list_objects(
        path STRING
    ) returns ARRAY api_integration = aws_stg_us_east_1_api max_batch_rows = 10 AS {% if target.name == "prod" %}
        'https://jfqhk99kj1.execute-api.us-east-1.amazonaws.com/stg/s3/list_objects'
    {% else %}
        'https://jfqhk99kj1.execute-api.us-east-1.amazonaws.com/stg/s3/list_objects'
    {%- endif %};
{% endmacro %}

{% macro create_udf_s3_copy_objects() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_s3_copy_objects(
        paths ARRAY,
        source STRING,
        target STRING
    ) returns ARRAY api_integration = aws_stg_us_east_1_api headers = (
        'overwrite' = '1'
    ) max_batch_rows = 1 AS {% if target.name == "prod" %}
        'https://jfqhk99kj1.execute-api.us-east-1.amazonaws.com/stg/s3/copy_objects'
    {% else %}
        'https://jfqhk99kj1.execute-api.us-east-1.amazonaws.com/stg/s3/copy_objects'
    {%- endif %};
{% endmacro %}

{% macro create_udf_s3_copy_objects_overwrite() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_s3_objects_overwrite(
        paths ARRAY,
        source STRING,
        target STRING
    ) returns ARRAY api_integration = aws_stg_us_east_1_api headers = (
        'overwrite' = '1'
    ) max_batch_rows = 1 AS {% if target.name == "prod" %}
        'https://jfqhk99kj1.execute-api.us-east-1.amazonaws.com/stg/s3/copy_objects'
    {% else %}
        'https://jfqhk99kj1.execute-api.us-east-1.amazonaws.com/stg/s3/copy_objects'
    {%- endif %};
{% endmacro %}
