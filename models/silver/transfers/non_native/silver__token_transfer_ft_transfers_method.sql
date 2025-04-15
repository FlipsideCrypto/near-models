{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::Date'],
    unique_key = 'transfers_id',
    incremental_strategy = 'delete+insert',
    tags = ['curated','scheduled_non_core']
) }}

WITH actions_events AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        signer_id,
        receiver_id,
        action_name,
        method_name,
        deposit,
        logs,
        receipt_succeeded,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__token_transfer_base') }}
    {% if var("MANUAL_FIX") %}
            WHERE {{ partition_load_manual('no_buffer') }}            
    {% elif is_incremental() %}
    WHERE modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
),
ft_transfers_method AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        receiver_id AS contract_address,
        REGEXP_SUBSTR(
            VALUE,
            'from ([^ ]+)',
            1,
            1,
            '',
            1
        ) :: STRING AS from_address,
        REGEXP_SUBSTR(
            VALUE,
            'to ([^ ]+)',
            1,
            1,
            '',
            1
        ) :: STRING AS to_address,
        REGEXP_SUBSTR(
            VALUE,
            '\\d+'
        ) :: variant AS amount_unadj,
        '' AS memo,
        b.index AS rn,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        actions_events
        JOIN LATERAL FLATTEN(
            input => logs
        ) b
    WHERE
        method_name = 'ft_transfer'
        AND from_address IS NOT NULL
        AND to_address IS NOT NULL
        AND amount_unadj IS NOT NULL
)
SELECT
    *,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash', 'action_id','contract_address','amount_unadj','from_address','to_address','memo','rn']
  ) }} AS transfers_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
    ft_transfers_method
