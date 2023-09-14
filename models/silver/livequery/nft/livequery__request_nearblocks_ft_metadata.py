import anyjson as json
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def model(dbt, session):

    dbt.config(
        materialized='incremental',
        unique_key='PAGE',
        packages=['anyjson'],
        tags=['livequery'],
        incremental_strategy='delete+insert'
    )


    # define settings variables
    page = 1
    url_params = {
        'page': page,
        'per_page': 50,
        'sort': 'holders',
        'order': 'desc'
    }

    # define parameters for udf_api
    method = 'GET'
    headers = {} # no header required for this api
    data = {} # arguments passed in URL via GET request
    base_url = 'https://api.nearblocks.io/v1/fts'

    # define result df schema with columns
    schema = T.StructType([
        T.StructField('PAGE', T.IntegerType()),
        T.StructField('REQUEST_SQL', T.StringType()),
        T.StructField('RESPONSE_COUNT', T.IntegerType()),
        T.StructField('DATA', T.VariantType())
    ])
    # TODO - upd cols to actuals and add inserted timestamp and response id as unique key

    # define result df
    result_df = session.create_dataframe(
        [],
        schema
    )

    # set max page at 5 for testing
    max_page = 100

    while True:
        # set url for GET request based on url_params
        url = base_url + '?' + '&'.join([f'{k}={v}' for k, v in url_params.items()])

        # call udf api (max 50 requests per page)
        call_udf_sql = f"select livequery.live.udf_api('{method}', '{url}', {headers}, {data})"

        # execute udf_api call
        # TODO - wrap in try-except?
        response = session.sql(call_udf_sql).collect()

        token_count = len(json.loads(response[0][0])['data']['tokens'])

        if token_count > 0:
            # append response to result df
            result_df = result_df.union(
                session.create_dataframe(
                    [
                        [
                            page,
                            call_udf_sql,
                            token_count,
                            response
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
