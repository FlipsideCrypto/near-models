## bronze__streamline_decoded_input_events.sql

This model selects and processes specific transaction data from the `near.silver.streamline_receipts_final` table. The primary focus is on transactions related to the `withdraw` method called by the `relay.aurora` signer. The model extracts and decodes event data from these transactions.

### Usage

This model is useful for extracting and decoding specific event data from transactions related to the withdraw method. The extracted data can then be further processed or analyzed as needed.


### Query Details

```sql
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
```

## Column Descriptions

- block_id: The unique identifier for the block containing the transaction.
- tx_hash: The unique hash of the transaction.
encoded_event: This field extracts the args from the first action in the receipt, which is expected to be a base64 encoded string. (It is also passed into the `decode_near_event()` function on the `streamline` side)
- event_struct: This is a predefined JSON string that maps field names to their corresponding data types. In this case, it specifies:

```sh
recipient_address: A 20-byte address.
amount: A 128-bit unsigned integer.
```
## Event Structure

The `event_struct` is used to define a `CStruct`, which specifies the structure of the binary data. Each field in the `event_struct` dictionary is mapped to its corresponding `borsh_consturct` data type and to decode near events.

### Example Usage
An example of the event_struct used in this model:

```json
{
    "recipient_address": "Bytes(20)",
    "amount": "U128"
}
```


This structure helps in defining how the binary data should be interpreted, ensuring that each field is correctly mapped to its corresponding data type.

### Supported Data Types

```sh
U128: A 128-bit unsigned integer.
Bytes(20): A 20-byte address.
```
