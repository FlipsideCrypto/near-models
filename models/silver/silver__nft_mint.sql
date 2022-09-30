{{ config(
  materialized = 'incremental',
  cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
  unique_key = 'action_id',
  incremental_strategy = 'delete+insert'
) }}


--data from silver_action_event table
WITH nft_mint AS (
    Select 
        action_id,
        tx_hash,
        block_id,
        block_timestamp,
        action_index,
        action_name,
        CASE 
             WHEN action_data :method_name in ('nft_mint', 'nft_mint_batch') THEN action_data :method_name :: STRING
            END AS method_name,
        _ingested_at,
        _inserted_timestamp
    FROM 
        {{ref ('silver__actions_events')}}
        
    where method_name in ('nft_mint', 'nft_mint_batch')
        

)
--Data pulled from action_events_function_call
,function_call_data AS (
  select 
        ACTION_ID,
        TX_HASH,
        BLOCK_ID,
        BLOCK_TIMESTAMP,
        try_parse_json(args) as args_json,
        METHOD_NAME,
        DEPOSIT,
        ATTACHED_GAS,
        
    Case 
     when method_name in (
        'nft_mint',
        'nft_batch_mint'
     ) THEN args_json:receiver_id::string
     when method_name in (
        'nft_mint',
        'nft_batch_mint'
     
     ) THEN args_json:receiver_ids :: STRING
             END AS Project_name,
    CASE 
      WHEN method_name in (
        'nft_mint',
        'nft_batch_mint'
      ) THEN try_parse_json ( --args_json :token_id :: STRING,
                        args_json :token_series_id :: STRING)
       WHEN method_name in (
        'nft_mint',
        'nft_batch_mint'
      ) THEN try_parse_json ( args_json :token_owner_id :: STRING)
     END AS nft_id,
     CASE 
      WHEN method_name in (
        'nft_mint',
        'nft_batch_mint'
      ) THEN try_parse_json ( args_json :token_id :: STRING) END AS token_id
    FROM
         {{ ref ('silver__actions_events_function_call') }}

    where method_name in ('nft_mint', 'nft_mint_batch')
    AND tx_hash in (
        SELECT Distinct 
         tx_hash from nft_mint)

 )
  --Data Pulled from Transaction 
,mint_transactions AS  (
select 
    tx_hash,
    tx_signer AS Signer,
    tx_receiver As receiver,
    transaction_fee as network_fee,
    GAS_USED,
    ATTACHED_GAS,
    tx_status,
    tx :actions[0] :FunctionCall :method_name :: STRING as method_name
    --try_parse_json(tx) :outcome :id :: STRING AS nft_id
FROM 
    {{ ref ('silver__transactions' )}}

where method_name in ('nft_mint','nft_mint_batch')
AND TX_STATUS = 'Success'
   
)
--Data pulled from Receipts Table
,receipts_data AS (
Select 
    
    Tx_Hash,
    Receipt_index,
    Receipt_object_id As Receipt_Id,
    Receipt_outcome_id :: STRING AS RECEIPT_OUTCOME_ID,
    Receiver_ID,
    Gas_Burnt
 FROM 
    {{ ref ('silver__receipts' )}}
WHERE 
    TX_HASH in (
    SELECT DISTINCT TX_Hash
    FROM nft_mint)

)


SELECT DISTINCT
        A.action_id,
        A.tx_hash,
        A.block_id,
        A.block_timestamp,
        A.action_index,
        A.action_name,
        A.method_name,
        A._ingested_at,
        A._inserted_timestamp,
        Signer,
        Receiver,
        project_name,
        token_id,
        nft_id,
        R.Receiver_id AS NFT_address,
        Deposit,
        B.Attached_GAS,
        Gas_used,
        Gas_Burnt,
        network_fee,
        R.Receipt_index,
        R.Receipt_Id,
        R.RECEIPT_OUTCOME_ID,
        T.tx_status
       
    FROM nft_mint A
    LEFT JOIN function_call_data B
    ON A.tx_hash = B.tx_hash
    LEFT JOIN mint_transactions T
    ON A.tx_hash = T.tx_hash
    LEFT JOIN receipts_data R
    ON A.tx_hash = R.tx_hash 
    where tx_status is not null