use near_dev.streamline;

CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_decode_near_events(
        json OBJECT
    ) returns ARRAY api_integration = aws_near_api_stg_v2 AS 
        'https://cx7cyhtcjf.execute-api.us-east-1.amazonaws.com/stg/bulk_decode_near_events';
    
grant usage on database near_dev to role aws_lambda_near_api;

 CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_decode_near_events(
        json OBJECT
    ) returns ARRAY api_integration = aws_near_api_stg_v2 AS 
        'https://cx7cyhtcjf.execute-api.us-east-1.amazonaws.com/stg/bulk_decode_near_events';

CREATE OR REPLACE EXTERNAL FUNCTION NEAR_DEV._LIVE.LT_BLOCKS_UDF_API("METHOD" VARCHAR(16777216), "URL" VARCHAR(16777216), "HEADERS" OBJECT, "DATA" VARIANT, "USER_ID" VARCHAR(16777216), "SECRET" VARCHAR(16777216))
RETURNS VARIANT
STRICT
API_INTEGRATION = "AWS_NEAR_API_STG_V2"
MAX_BATCH_ROWS = 25
HEADERS = ('fsc-compression-mode' = 'auto')
AS 'https://cx7cyhtcjf.execute-api.us-east-1.amazonaws.com/stg/udf_api';