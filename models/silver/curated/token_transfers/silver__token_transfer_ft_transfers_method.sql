
{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::Date'],
    unique_key = 'transfers_id',
    incremental_strategy = 'merge',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,action_id,contract_address,from_address,to_address);",
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
        modified_timestamp AS _modified_timestamp,
        _partition_by_block_number
    FROM
        {{silver__token_transfer_base}}
    {% if is_incremental() %}
    WHERE _modified_timestamp >= (
        SELECT
            MAX(_modified_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
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
        ) :: variant AS amount_unadjusted,
        '' AS memo,
        b.index AS rn,
        _inserted_timestamp,
        _modified_timestamp,
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
        AND amount_unadjusted IS NOT NULL
)
SELECT
    *
FROM
    ft_transfers_method