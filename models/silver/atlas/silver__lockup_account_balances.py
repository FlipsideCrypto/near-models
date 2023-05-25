import snowflake.snowpark as snowpark
# import logging

# logger = logging.getLogger("python_logger")
# logger.info("Logging test from Python module.")
# logger.error("Logging an error from Python handler")

def request(df, headers, session):
    # params for api call
    base_accounts_url = "https://near-mainnet.api.pagoda.co/eapi/v1/accounts/"
    api_route = "/balances/NEAR"
    block_parameter = "?block_height="
    # temp block id input, max on May 22
    block_height = "92485306"

    schema = snowpark.session.StructType(
        [
            snowpark.types.StructField("BLOCK_HEIGHT", snowpark.types.StringType()),
            snowpark.types.StructField("LOCKUP_ACCOUNT_ID", snowpark.types.StringType()),
            snowpark.types.StructField("RESPONSE", snowpark.types.VariantType())
        ])

    response_df = session.createDataFrame([], schema)

    for row in df.collect():
        # TODO add dynamic block height
        url = base_accounts_url + row["LOCKUP_ACCOUNT_ID"] + api_route + block_parameter + block_height
        try:
            sql = f"""
                select
                    {block_height} as BLOCK_HEIGHT,
                    '{row['LOCKUP_ACCOUNT_ID']}' as LOCKUP_ACCOUNT_ID,
                    ethereum.streamline.udf_api(
                        'GET',
                        '{url}',
                        {headers},
                        {dict()}
                    ) as RESPONSE
            """
            response_df = response_df.union(session.sql(sql))

            # response = {
            #     "account_id": row["LOCKUP_ACCOUNT_ID"],
            #     "block_height": block_height,
            #     "data": r.json(),
            #     "error": None,
            #     "status_code": r.status_code,
            # }

        except Exception as e:
            response_df = response_df.union(
                session.createDataFrame([{
                    "BLOCK_HEIGHT": block_height, 
                    "LOCKUP_ACCOUNT_ID": row["LOCKUP_ACCOUNT_ID"], 
                    "RESPONSE": {"error": str(e)}
                }],
                schema)
            )
    # logger.info("Test info log from request function")

    return response_df

def model(dbt, session):

    dbt.config(
        # TODO reconfig to incremental
        materialized='table'
    )

    # TODO store this securely
    PAGODA_KEY = ""

    headers = {
        "Content-Type": "application/json",
        "x-api-key": PAGODA_KEY
    }


    # upstream tables
    # configure df for lockup_accounts
    lockup_accounts = dbt.ref("silver__lockup_accounts")

    # filter to active accounts only, based on is_deleted column
    active_lockup_accounts = lockup_accounts.filter(lockup_accounts.is_deleted == False)
    account_sample = active_lockup_accounts.limit(100)
    

    # call api via request function
    test = request(
        account_sample,
        headers,
        session
    )

    return test