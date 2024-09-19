{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_decode_near_events',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"DECODED_INPUT_EVENTS",
        "sql_limit" :"500",
        "producer_batch_size" :"50",
        "worker_batch_size" :"10",
        "sql_source" :"{{this.identifier}}" }
    ),
) }}

SELECT 
    block_id,
    tx_hash,
    receipt_actions:receipt:Action:actions[0]:FunctionCall:args::string as encoded_event,
    '{"recipient_address": "Bytes(20)", "amount": "U128"}' as event_struct
FROM near.silver.streamline_receipts_final
WHERE block_timestamp >= sysdate() - INTERVAL '2 weeks'
AND signer_id = 'relay.aurora'
AND object_keys(receipt_actions:receipt:Action:actions[0])[0] = 'FunctionCall'
AND receipt_actions:receipt:Action:actions[0]:FunctionCall:method_name::STRING = 'withdraw'
LIMIT 100