import requests as res
import snowflake.snowpark as snowpark
import logging

logger = logging.getLogger("python_logger")
# logger.info("Logging from Python module.")
# logger.error("Logging an error from Python handler: ")

def request(df, headers, session):
    # params for api call
    base_accounts_url = "https://near-mainnet.api.pagoda.co/eapi/v1/accounts/"
    api_route = "/balances/NEAR"
    block_parameter = "?block_height="
    # temp block id input, max on May 22
    block_height = 92485306

    schema = snowpark.session.StructType(
        [
            snowpark.types.StructField("LOCKUP_ACCOUNT_ID", snowpark.types.StringType()),
            snowpark.types.StructField("RESPONSE", snowpark.types.VariantType())
        ])

    values = []

    for row in df.collect():
        url = base_accounts_url + row["LOCKUP_ACCOUNT_ID"] + api_route + block_parameter + str(block_height)
        try:
            r = res.get(url, headers=headers)

            response = {
                "account_id": row["LOCKUP_ACCOUNT_ID"],
                "data": r.json(),
                "error": None,
                "status_code": r.status_code,
            }

        except Exception as e:
            response = {
                "account_id": row["LOCKUP_ACCOUNT_ID"],
                "data": None,
                "error": str(e),
                "status_code": None
            }

        values.append(
            {
                "LOCKUP_ACCOUNT_ID": row["LOCKUP_ACCOUNT_ID"],
                "RESPONSE": response
            }
        )

    return session.createDataFrame(values, schema)

def model(dbt, session):

    dbt.config(
        # TODO reconfig to incremental
        materialized='table',
        packages=['requests']
    )
    logger.info("Test INFO log from dbt model")
    logger.info("Test ERROR log from dbt model")

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
    account_sample = active_lockup_accounts.limit(10)
    
    # try adding a new col
    # add_col = account_sample.withColumn(
    #     "res", 
    #     request(
    #         account_sample,
    #         headers,
    #         session
    #     ).RESPONSE
    #     )

    # call api via request function
    test = request(
        account_sample,
        headers,
        session
    )


    return test