{{ config(
    materialized = 'view',
    tags = ['core', 'horizon']
) }}

WITH horizon AS (

    SELECT
        action_id_horizon,
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
        receipt_succeeded,
        COALESCE(
            horizon_decoded_actions_id,
            {{ dbt_utils.generate_surrogate_key(
                ['action_id_horizon']
            ) }}
        ) AS fact_decoded_actions_id,
        COALESCE(inserted_timestamp,'2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
    FROM
        {{ ref('silver_horizon__decoded_actions') }}
    WHERE
        method_name != 'set'
)
SELECT
    *
FROM
    horizon
