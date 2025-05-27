use near_dev.streamline;
show user functions;
drop function streamline.UDF_DECODE_NEAR_EVENT(VARCHAR);

CREATE
    OR REPLACE EXTERNAL FUNCTION near_dev.streamline.udf_decode_near_event(
        DATA STRING, EVENT_STRUCT STRING
    ) returns ARRAY api_integration = aws_near_api_stg_v2 AS
        'https://cx7cyhtcjf.execute-api.us-east-1.amazonaws.com/stg/decode_near_event';  

CREATE
    OR REPLACE EXTERNAL FUNCTION near_dev.streamline.udf_bulk_decode_near_events(
        json OBJECT
    ) returns ARRAY api_integration = aws_near_api_stg_v2 AS 
        'https://cx7cyhtcjf.execute-api.us-east-1.amazonaws.com/stg/bulk_decode_near_events';

show grants on integration AWS_NEAR_API_STG_V2;

desc integration AWS_NEAR_API_STG_V2;

show user functions;

desc function streamline.UDF_BULK_DECODE_NEAR_EVENTS(OBJECT);

grant usage on database near_dev to role AWS_LAMBDA_NEAR_API;

grant select on all views in schema near_dev.streamline to role AWS_LAMBDA_NEAR_API;