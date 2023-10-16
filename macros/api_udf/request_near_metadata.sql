{% macro request_near_metadata() %}

{% set create_table %}
CREATE SCHEMA IF NOT EXISTS {{ target.database }}.bronze_api;
CREATE TABLE IF NOT EXISTS {{ target.database }}.bronze_api.near_metadata(
    tx_hash STRING,
    block_id STRING,
    contract_account_id STRING,
    token_id STRING,
    series_id STRING,
    metadata_id STRING,
    res_url STRING,
    lq_response VARIANT,
    call_succeeded BOOLEAN,
    error_code INTEGER,
    error_message STRING,
    _request_timestamp TIMESTAMP_NTZ,
    _inserted_timestamp TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS {{ target.database }}.bronze_api.log_table(
    log_id INTEGER AUTOINCREMENT,
    log_message STRING,
    log_timestamp TIMESTAMP_NTZ
);

{% endset %}

{% do run_query(create_table) %}

{% set query %}
CREATE OR REPLACE PROCEDURE {{ target.database }}.bronze_api.get_near_metadata() 
    RETURNS STRING 
    LANGUAGE JAVASCRIPT 
    EXECUTE AS CALLER 
    AS $$
    
    const MAX_CALLS = 100;
    const BATCH_SIZE = 10;
    const CALL_GROUPS = Math.ceil(MAX_CALLS / BATCH_SIZE);


    // Create a temporary table for metadata_ids not in near_metadata
    var create_metadata_id_temp_table = `
        CREATE OR REPLACE TABLE {{ target.database }}.bronze_api.metadata_ids_not_in_near_metadata AS
        WITH nfts_minted AS (
            SELECT
                tx_hash,
                block_id,
                receiver_id AS contract_account_id,
                token_id,
                IFF(
                    SPLIT(token_id, ':')[1] IS NULL,
                    contract_account_id,
                    SPLIT(token_id, ':')[0]
                ) AS series_id,
                MD5(receiver_id || series_id) AS metadata_id,
                COALESCE(_inserted_timestamp, _load_timestamp) AS _inserted_timestamp
            FROM {{ ref('silver__standard_nft_mint_s3') }}
            WHERE contract_account_id NOT IN ('nft-assets.l2e.near')
        )
        SELECT 
            *
        FROM nfts_minted
        WHERE metadata_id NOT IN (SELECT metadata_id FROM {{ target.database }}.bronze_api.near_metadata)
        ORDER BY metadata_id
        LIMIT ${MAX_CALLS};
    `;
    
    snowflake.execute({sqlText: create_metadata_id_temp_table});

    for(let group = 0; group < CALL_GROUPS; group++) {

        var create_temp_table_command = `
            CREATE OR REPLACE TEMPORARY TABLE {{ target.database }}.bronze_api.response_data AS
            WITH api_call AS (
        `;

        for (let i = 0; i < BATCH_SIZE; i++) {
            create_temp_table_command += `
                SELECT * FROM (
                    SELECT
                        tx_hash,
                        block_id,
                        contract_account_id,
                        token_id,
                        series_id,
                        metadata_id,
                        'https://near-mainnet.api.pagoda.co/eapi/v1/NFT/' || contract_account_id || '/' || token_id AS res_url,
                        ethereum.streamline.udf_api(
                            'GET',
                            res_url,
                            { 
                                'x-api-key': '17847af4-c948-4690-beae-bafa77429822', 
                                'Content-Type': 'application/json'
                            },
                            {}
                        ) AS lq_response,
                        SYSDATE() AS _request_timestamp,
                        _inserted_timestamp
                    FROM {{ target.database }}.bronze_api.metadata_ids_not_in_near_metadata
                    LIMIT 1 OFFSET ${i + (BATCH_SIZE * group)}
                )
            `;

            if (i < BATCH_SIZE - 1) {
                create_temp_table_command += ` UNION ALL `;
            }
        }

        create_temp_table_command += `
            ) -- This closes the api_call CTE
            SELECT * FROM api_call;
                `;
        snowflake.execute({sqlText: create_temp_table_command});

        var insert_command = `
            INSERT INTO {{ target.database }}.bronze_api.near_metadata
            SELECT
                tx_hash,
                block_id,
                contract_account_id,
                token_id,
                series_id,
                metadata_id,
                res_url,
                lq_response,
                (lq_response :data :message IS NULL) AND (lq_response :error IS NULL) AS call_succeeded,
                COALESCE(lq_response :data :code :: INT, lq_response :status_code :: INT) AS error_code,
                lq_response :data :message :: STRING AS error_message,
                _request_timestamp,
                _inserted_timestamp
            FROM {{ target.database }}.bronze_api.response_data;
        `;

        snowflake.execute({sqlText: insert_command});

        var log_message = `Processing batch ${group + 1}`;
        var insert_log_command = `
            INSERT INTO {{ target.database }}.bronze_api.log_table(log_message, log_timestamp)
            VALUES ('${log_message}', CURRENT_TIMESTAMP());
        `;

        // Execute the command
        snowflake.execute({sqlText: insert_log_command});
    }

    return 'Success';

$$;
{% endset %}
{% do run_query(query) %}


{% endmacro %}