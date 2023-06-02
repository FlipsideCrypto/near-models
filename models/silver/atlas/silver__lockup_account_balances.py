import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from datetime import datetime


def register_udf_construct_url():
    construct_url = (
        F.udf(
            lambda base_url, account_id, block_id: base_url.replace('{account_id}', account_id).replace('{block_id}', str(block_id)), 
            name='construct_url', 
            input_types=[
                snowpark.types.StringType(), 
                snowpark.types.StringType(), 
                snowpark.types.IntegerType()
                ], 
            return_type=snowpark.types.StringType(), 
            if_not_exists=True
        )
    )

    return construct_url


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
    schema = T.StructType(
            [
                T.StructField('BLOCK_DATE', T.DateType()),
                T.StructField('BLOCK_ID', T.IntegerType()),
                T.StructField('LOCKUP_ACCOUNT_ID', T.StringType()),
                T.StructField('RESPONSE', T.VariantType()),
                T.StructField('_REQUEST_TIMESTAMP', T.TimestampType()),
                T.StructField('_RES_ID', T.StringType())
            ]
        )

    # create empty df to store response
    response_df = session.create_dataframe([], schema)

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
                    CURRENT_TIMESTAMP as _REQUEST_TIMESTAMP,
                    CONCAT_WS('-', LOCKUP_ACCOUNT_ID, BLOCK_ID) as _RES_ID
            """

            try:
                # execute sql via collect() and append to response
                r = session.sql(sql).collect()
                response_df = response_df.union(session.createDataFrame(r, schema))

            except Exception as e:
                error_count += 1

                # log error in table
                response_df = response_df.union(
                    session.create_dataframe(
                        [
                            {
                                'BLOCK_DATE': block_id['BLOCK_DATE'],
                                'BLOCK_ID': block_id['BLOCK_ID'], 
                                'LOCKUP_ACCOUNT_ID': account_id['LOCKUP_ACCOUNT_ID'], 
                                'RESPONSE': {
                                        'error': str(e)
                                    },
                                '_REQUEST_TIMESTAMP': datetime.now(),
                                '_RES_ID': f"{account_id['LOCKUP_ACCOUNT_ID']}-{block_id['BLOCK_ID']}"
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
        materialized='incremental',
        unique_key='_RES_ID'
    )

    # configure upstream tables
    lockup_accounts = dbt.ref('silver__lockup_accounts')

    blocks_to_query = dbt.ref('silver__streamline_blocks')
    blocks_to_query = blocks_to_query.group_by(
            F.date_trunc('DAY', 'BLOCK_TIMESTAMP')
        ).agg(
            F.max('BLOCK_ID').as_('BLOCK_ID')
        ).with_column_renamed(
            'DATE_TRUNC(DAY, BLOCK_TIMESTAMP)', 'BLOCK_DATE'
        ).where(
            'BLOCK_DATE != CURRENT_DATE'
        )

    # limit scope of query for testing
    lockup_accounts = lockup_accounts.order_by('LOCKUP_ACCOUNT_ID').limit(5)

    # use the first date range on full-refresh to load a range
    blocks_to_query = blocks_to_query.where("BLOCK_DATE BETWEEN '2023-05-25' AND '2023-05-28'")
    # use the below to test incremental load
    # blocks_to_query = blocks_to_query.where("BLOCK_DATE >= '2023-05-25'")

    # define incremental logic
    if dbt.is_incremental:

        max_from_this = f"select max(BLOCK_DATE) from {dbt.this}"
        blocks_to_query = blocks_to_query.filter(blocks_to_query.BLOCK_DATE >= session.sql(max_from_this).collect()[0][0])

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
