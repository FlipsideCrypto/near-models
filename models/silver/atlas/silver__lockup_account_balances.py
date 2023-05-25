import snowflake.snowpark as snowpark
# import logging

# logger = logging.getLogger("python_logger")
# logger.info("Logging test from Python module.")
# logger.error("Logging an error from Python handler")

def request(session, base_url, df=None, parameters=None):
    """
    Function to call the UDF_API.
    df (optional) - Snowpark DataFrame of input data.
    parameters (optional) - string of parameters to append to base_url
    """

    # temp hard-coded params
    # TODO store this securely
    PAGODA_KEY = "17847af4-c948-4690-beae-bafa77429822"
    # single block id input, max block on May 22
    block_height = "92485306"

    # params for UDF_API
    method = 'GET'
    headers = {
        "Content-Type": "application/json",
        "x-api-key": PAGODA_KEY
    }
    data = {}

    # define schema for response df
    schema = snowpark.session.StructType(
        [
            snowpark.types.StructField("BLOCK_HEIGHT", snowpark.types.IntegerType()),
            snowpark.types.StructField("LOCKUP_ACCOUNT_ID", snowpark.types.StringType()),
            snowpark.types.StructField("RESPONSE", snowpark.types.VariantType())
        ])

    # create empty df to store response
    response_df = session.createDataFrame([], schema)

    # loop through df and call api via udf
    # TODO - change to request batch of N at a time, instead of 1 by 1
    # TODO - adjust param to backfill past block heights (2nd upstream table?)
    for row in df.collect():

        # url = base_url.replace("{account_id}", row["LOCKUP_ACCOUNT_ID"]) + parameters.replace("{block_height}", block_height) if parameters else base_url.replace("{account_id}", row["LOCKUP_ACCOUNT_ID"])
        url = base_url.replace("{account_id}", row["LOCKUP_ACCOUNT_ID"]) + parameters.replace("{block_height}", block_height)

        sql = f"""
            select
                {block_height}::INT as BLOCK_HEIGHT,
                '{row['LOCKUP_ACCOUNT_ID']}' as LOCKUP_ACCOUNT_ID,
                ethereum.streamline.udf_api(
                    '{method}',
                    '{url}',
                    {headers},
                    {data}
                ) as RESPONSE
        """

        try:
            # https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/api/snowflake.snowpark.Session.sql.html
            # session.sql creates the df, .collect() must be called to actually execute the query
            r = session.sql(sql).collect()
            row_df = session.createDataFrame(r, schema)
            response_df = response_df.union(row_df)

        except Exception as e:
            response_df = response_df.union(
                session.createDataFrame([{
                    "BLOCK_HEIGHT": block_height, 
                    "LOCKUP_ACCOUNT_ID": row["LOCKUP_ACCOUNT_ID"], 
                    "RESPONSE": {"error": str(e)}
                }],
                schema)
            )
            # TODO - error behavior? maybe raise after N errors 

    return response_df


def model(dbt, session):

    dbt.config(
        # TODO reconfig to incremental
        materialized='table'
    )

    # upstream tables

    # configure df for lockup_accounts
    lockup_accounts = dbt.ref("silver__lockup_accounts")

    # filter to active accounts only, based on is_deleted column
    active_lockup_accounts = lockup_accounts.filter(lockup_accounts.is_deleted == False)
    account_sample = active_lockup_accounts.limit(5)
    

    # call api via request function
    base_url = "https://near-mainnet.api.pagoda.co/eapi/v1/accounts/{account_id}/balances/NEAR"
    df = account_sample
    parameters = "?block_height={block_height}"

    test = request(
        session,
        base_url,
        df,
        parameters
    )

    # dbt models return a df which is written as a table
    return test
