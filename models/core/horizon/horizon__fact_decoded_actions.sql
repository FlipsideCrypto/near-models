{{ config(
    materialized = 'view',
    tags = ['core', 'horizon']
) }}

WITH horizon AS (

    SELECT
        receipt_object_id,
        tx_hash,
        block_id,
        block_timestamp,
        method_name,
        args,
        deposit,
        attached_gas,
        receiver_id,
        signer_id,
        receipt_succeeded
    FROM
        {{ ref('silver_horizon__decoded_actions') }}
    WHERE
        method_name != 'set'
)
SELECT
    *
FROM
    horizon
