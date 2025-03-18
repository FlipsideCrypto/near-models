import anyjson as json
import snowflake.snowpark.types as T

from datetime import datetime


def model(dbt, session):

    dbt.config(
        materialized='incremental',
        unique_key='_RES_ID',
        packages=['anyjson'],
        tags=['livequery', 'nearblocks'],
        incremental_strategy='delete+insert'
    )

    # define api parameters
    page = 1
    url_params = {
        'page': page,
        'per_page': 50,
        'sort': 'holders',
        'order': 'desc'
    }

    # define parameters for udf_api
    method = 'GET'
    headers = {}  # no header required for this api
    data = {}  # arguments passed in URL via GET request
    base_url = 'https://api.nearblocks.io/v1/nfts'

    # define result df schema with columns
    schema = T.StructType([
        T.StructField('PAGE', T.IntegerType()),
        T.StructField('REQUEST_SQL', T.StringType()),
        T.StructField('RESPONSE_COUNT', T.IntegerType()),
        T.StructField('DATA', T.VariantType()),
        T.StructField('_INSERTED_TIMESTAMP', T.TimestampType()),
        T.StructField('_RES_ID', T.StringType())
    ])

    # define result df
    result_df = session.create_dataframe(
        [],
        schema
    )

    # set upper limit
    max_page = 100

    while True:
        # set url for GET request based on url_params
        url = f'{base_url}?{"&".join([f"{k}={v}" for k, v in url_params.items()])}'

        # call udf api (max 50 requests per page)
        call_udf_sql = f"select livequery.live.udf_api('{method}', '{url}', {headers}, {data})"

        # execute udf_api call
        response = session.sql(call_udf_sql).collect()
        try:
            token_count = len(json.loads(response[0][0])['data']['tokens'])
        except:
            break
        if token_count > 0:

            _inserted_timestamp = datetime.utcnow()
            _res_id = f'{_inserted_timestamp.date()}-{page}'

            # append response to result df
            result_df = result_df.union(
                session.create_dataframe(
                    [
                        [
                            page,
                            call_udf_sql,
                            token_count,
                            json.loads(response[0][0]),
                            _inserted_timestamp,
                            _res_id
                        ]
                    ],
                    schema
                )
            )

            # increment page
            page += 1

            # update data
            url_params.update({'page': page})

            # break if max page reached
            if page > max_page:
                break

        else:
            break

    return result_df
