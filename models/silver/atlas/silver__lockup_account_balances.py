import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def try_call_udf_api(method, url, headers, data):
    """
    A wrapper function on the UDF_API to add a try/except block to catch and log errors.
    """

    try:
        res = F.call_udf('ethereum.streamline.udf_api', method, url, headers, data)

    except Exception as e:
        res = {'error': str(e)}

    return res



def register_udf_construct_url():
    """
    Helper function to register an anonymous UDF to construct the URL for the API call.
    This particular URL requires 2 inputs from another table, account_id and block_id, with are passed in as both a URL path and parameter.
    The inputs will be passed to the UDF, not to the registration function, thus the function has no arguments while the UDF has 3, as indicated by the array input_types.
    """
    construct_url = (
        F.udf(
            lambda base_url, account_id, block_id: base_url.replace('{account_id}', account_id).replace('{block_id}', str(block_id)), 
            name='construct_url', 
            input_types=[
                T.StringType(), 
                T.StringType(), 
                T.IntegerType()
            ], 
            return_type=T.StringType(), 
            if_not_exists=True
        )
    )

    return construct_url


def request(session, base_url, api_key=None, df=None):
    """
    Function to call the UDF_API.
    df (optional) - Snowpark DataFrame of input data
    """

    # define params for UDF_API
    method = 'GET'
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': api_key
    }
    data = {}

    # instantiate object for helper udf(s)
    construct_url = register_udf_construct_url()

    response_df = df.with_columns(
        ["RESPONSE", "_REQUEST_TIMESTAMP", "_RES_ID"],
        [
            try_call_udf_api(
                method,
                construct_url(
                    F.lit(base_url),
                    F.col('LOCKUP_ACCOUNT_ID'),
                    F.col('BLOCK_ID')
            ),
            headers,
            data
            ),
            F.current_timestamp(),
            F.concat_ws(
                F.lit('-'),
                F.col('LOCKUP_ACCOUNT_ID'),
                F.col('BLOCK_ID')
            )
        ]
    )
    
    return response_df


def model(dbt, session):

    dbt.config(
        materialized='incremental',
        unique_key='_RES_ID',
        packages=['snowflake-snowpark-python']
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
    # blocks_to_query = blocks_to_query.where("BLOCK_DATE BETWEEN '2023-05-25' AND '2023-05-28'")
    # use the below to test incremental load
    blocks_to_query = blocks_to_query.where("BLOCK_DATE >= '2023-05-25'")

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
    api_key = session.sql("select * from near._internal.api_key where platform = 'pagoda'").collect()[0]['API_KEY']
    df = active_accounts


    # TODO - add error threshold
    error_count = 0


    final_df = request(
        session,
        base_url,
        api_key,
        df
    )

    # dbt models return a df which is written as a table
    return final_df
