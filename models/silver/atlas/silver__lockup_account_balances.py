import snowflake.snowpark as snowpark
from datetime import datetime

def request(session, base_url, df=None):
    """
    Function to call the UDF_API.
    df (optional) - Snowpark DataFrame of input data
    """

    # define params for UDF_API
    API_KEY = session.sql("select * from near._internal.api_key where platform = 'pagoda'").collect()[0]['API_KEY']

    method = 'GET'
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': API_KEY
    }
    data = {}

    # define schema for response df
    schema = snowpark.session.StructType(
            [
                snowpark.types.StructField('BLOCK_DATE', snowpark.types.DateType()),
                snowpark.types.StructField('BLOCK_ID', snowpark.types.IntegerType()),
                snowpark.types.StructField('LOCKUP_ACCOUNT_ID', snowpark.types.StringType()),
                snowpark.types.StructField('RESPONSE', snowpark.types.VariantType()),
                snowpark.types.StructField('_REQUEST_TIMESTAMP', snowpark.types.TimestampType())
            ]
        )

    # create empty df to store response
    response_df = session.createDataFrame([], schema)

    # loop through df and call api via udf
    # TODO - change to request batch of N at a time, instead of 1 by 1

    error_count = 0

    for block_id in df.select('BLOCK_ID', 'BLOCK_DATE').distinct().order_by('BLOCK_ID').collect():
        
        for account_id in df.select('LOCKUP_ACCOUNT_ID').where(f"BLOCK_ID = {block_id['BLOCK_ID']}").collect():
            
            url = base_url.replace(
                    "{account_id}", account_id['LOCKUP_ACCOUNT_ID']
                ).replace(
                    "{block_id}", str(block_id['BLOCK_ID'])
                )

            sql = f"""
                select
                    '{block_id['BLOCK_DATE']}'::DATE as BLOCK_DATE,
                    {block_id['BLOCK_ID']}::INT as BLOCK_ID,
                    '{account_id['LOCKUP_ACCOUNT_ID']}' as LOCKUP_ACCOUNT_ID,
                    ethereum.streamline.udf_api(
                        '{method}',
                        '{url}',
                        {headers},
                        {data}
                    ) as RESPONSE,
                    CURRENT_TIMESTAMP as _REQUEST_TIMESTAMP
            """

            try:
                r = session.sql(sql).collect()
                row_df = session.createDataFrame(r, schema)
                response_df = response_df.union(row_df)

            except Exception as e:
                error_count += 1

                response_df = response_df.union(
                    session.createDataFrame(
                        [
                            {
                                'BLOCK_DATE': block_id['BLOCK_DATE'],
                                'BLOCK_ID': block_id['BLOCK_ID'], 
                                'LOCKUP_ACCOUNT_ID': account_id['LOCKUP_ACCOUNT_ID'], 
                                'RESPONSE': {
                                        'error': str(e)
                                    },
                                '_REQUEST_TIMESTAMP': datetime.now()
                            }
                        ],
                        schema
                    )
                )

                # arbitrary limit of 10 errors
                if error_count >= 10:
                    raise Exception(f"Too many errors - {error_count}")

    return response_df

def model(dbt, session):

    dbt.config(
        materialized='table',
        unique_key='LOCKUP_ACCOUNT_ID'
    )

    # configure upstream tables
    lockup_accounts = dbt.ref('silver__lockup_accounts')

    blocks_to_query = dbt.ref('silver__streamline_blocks')
    blocks_to_query = blocks_to_query.groupBy(
            snowpark.functions.date_trunc('DAY', 'BLOCK_TIMESTAMP')
        ).agg(
            snowpark.functions.max('BLOCK_ID').as_('BLOCK_ID')
        ).with_column_renamed(
            'DATE_TRUNC(DAY, BLOCK_TIMESTAMP)', 'BLOCK_DATE'
        ).where(
            'BLOCK_DATE != CURRENT_DATE'
        )

    # limit scope of query for testing
    lockup_accounts = lockup_accounts.order_by('LOCKUP_ACCOUNT_ID').limit(3)
    blocks_to_query = blocks_to_query.where("BLOCK_DATE >= '2023-05-25'")

    active_accounts = lockup_accounts.select(
            'LOCKUP_ACCOUNT_ID', 'DELETION_BLOCK_ID'
        ).cross_join(
            blocks_to_query
        ).where(
            'COALESCE(DELETION_BLOCK_ID, 1000000000000) >= BLOCK_ID'
        )

    # call api via request function
    base_url = 'https://near-mainnet.api.pagoda.co/eapi/v1/accounts/{account_id}/balances/NEAR?block_height={block_id}'
    df = active_accounts

    final_df = request(
        session,
        base_url,
        df
    )

    # dbt models return a df which is written as a table
    return final_df
