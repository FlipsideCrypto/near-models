{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'transfers_id',
    incremental_strategy = 'merge',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,action_id,contract_address,from_address,to_address);",
    tags = ['curated','scheduled_non_core']
) }}
{# Note - multisource model #}
-- Curation Challenge - 'https://flipsidecrypto.xyz/Hossein/transfer-sector-of-near-curation-challenge-zgM44F'

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
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        receipt_succeeded = TRUE
        AND logs [0] IS NOT NULL
        AND RIGHT(action_id, 2) = '-0' 

        {% if var("MANUAL_FIX") %}
            AND {{ partition_load_manual('no_buffer') }}
        {% else %}
{% if is_incremental() %}
AND _modified_timestamp >= (
    SELECT
        MAX(_modified_timestamp)
    FROM
        {{ this }}
    WHERE
        transfer_type = 'nep141'
)
{% endif %}
{% endif %}
),
----------------------------    Native Token Transfers   ------------------------------
native_transfers AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        predecessor_id AS from_address,
        receiver_id AS to_address,
        IFF(REGEXP_LIKE(deposit, '^[0-9]+$'), deposit, NULL) AS amount_unadjusted,
        --numeric validation (there are some exceptions that needs to be ignored)
        receipt_succeeded,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__transfers_s3') }}
    WHERE
        status = TRUE AND deposit != 0 

        {% if var("MANUAL_FIX") %}
            AND {{ partition_load_manual('no_buffer') }}
        {% else %}
    {% if is_incremental() %}
    AND _modified_timestamp >= (
        SELECT
            MAX(_modified_timestamp)
        FROM
            {{ this }}
        WHERE
            transfer_type = 'native'

)
{% endif %}
{% endif %}
),
------------------------------   NEAR Tokens (NEP 141) --------------------------------
orders AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        receiver_id,
        TRY_PARSE_JSON(REPLACE(g.value, 'EVENT_JSON:')) AS DATA,
        DATA :event :: STRING AS event,
        g.index AS rn,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        actions_events
        JOIN LATERAL FLATTEN(
            input => logs
        ) g
    WHERE
        DATA :event :: STRING = 'order_added'
),
orders_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        f.value :sell_token :: STRING AS contract_address,
        f.value :owner_id :: STRING AS from_address,
        receiver_id :: STRING AS to_address,
        (
            f.value :original_amount
        ) :: variant AS amount_unadjusted,
        'order' AS memo,
        f.index AS rn,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        orders
        JOIN LATERAL FLATTEN(
            input => DATA :data
        ) f
    WHERE
        amount_unadjusted > 0
),
add_liquidity AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        REGEXP_SUBSTR(
            SPLIT.value,
            '"\\d+ ([^"]*)["]',
            1,
            1,
            'e',
            1
        ) :: STRING AS contract_address,
        NULL AS from_address,
        receiver_id AS to_address,
        REGEXP_SUBSTR(
            SPLIT.value,
            '"(\\d+) ',
            1,
            1,
            'e',
            1
        ) :: variant AS amount_unadjusted,
        'add_liquidity' AS memo,
        INDEX AS rn,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        actions_events,
        LATERAL FLATTEN (
            input => SPLIT(
                REGEXP_SUBSTR(
                    logs [0],
                    '\\["(.*?)"\\]'
                ),
                ','
            )
        ) SPLIT
    WHERE
        logs [0] LIKE 'Liquidity added [%minted % shares'
),
ft_transfers_method AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        receiver_id AS contract_address,
        regexp_substr(VALUE, 'from ([^ ]+)', 1, 1, '', 1) :: STRING  as from_address,
        regexp_substr(VALUE, 'to ([^ ]+)', 1, 1, '', 1):: STRING as to_address,
        regexp_substr(VALUE, '\\d+') :: VARIANT as amount_unadjusted,
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
        AND from_address IS NOT NULL AND to_address IS NOT NULL AND amount_unadjusted IS NOT NULL
),
ft_transfers_event AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        TRY_PARSE_JSON(REPLACE(VALUE, 'EVENT_JSON:')) AS DATA,
        b.index AS logs_rn,
        receiver_id AS contract_address,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        actions_events
        JOIN LATERAL FLATTEN(
            input => logs
        ) b
    WHERE
        DATA :event :: STRING IN (
            'ft_transfer'
        )
),
ft_transfers_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        contract_address,
        NVL(
            f.value :old_owner_id,
            NULL
        ) :: STRING AS from_address,
        NVL(
            f.value :new_owner_id,
            f.value :owner_id
        ) :: STRING AS to_address,
        f.value :amount :: variant AS amount_unadjusted,
        f.value :memo :: STRING AS memo,
        logs_rn + f.index AS rn,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        ft_transfers_event
        JOIN LATERAL FLATTEN(
            input => DATA :data
        ) f
    WHERE
        amount_unadjusted > 0
),
ft_mints AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        TRY_PARSE_JSON(REPLACE(VALUE, 'EVENT_JSON:')) AS DATA,
        b.index AS logs_rn,
        receiver_id AS contract_address,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        actions_events
        JOIN LATERAL FLATTEN(
            input => logs
        ) b
    WHERE
        DATA :event :: STRING IN (
            'ft_mint'
        )
),
ft_mints_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        contract_address,
        NVL(
            f.value :old_owner_id,
            NULL
        ) :: STRING AS from_address,
        NVL(
            f.value :new_owner_id,
            f.value :owner_id
        ) :: STRING AS to_address,
        f.value :amount :: variant AS amount_unadjusted,
        f.value :memo :: STRING AS memo,
        logs_rn + f.index AS rn,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        ft_mints
        JOIN LATERAL FLATTEN(
            input => DATA :data
        ) f
    WHERE
        amount_unadjusted > 0
),
nep_transfers AS (
    SELECT
        *
    FROM
        ft_transfers_method
    UNION ALL
    SELECT
        *
    FROM
        ft_transfers_final
    UNION ALL
    SELECT
        *
    FROM
        ft_mints_final
    UNION ALL
    SELECT
        *
    FROM
        orders_final
    UNION ALL
    SELECT
        *
    FROM
        add_liquidity
),
------------------------------  MODELS --------------------------------

native_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        'wrap.near' AS contract_address,
        from_address :: STRING,
        to_address :: STRING,
        NULL AS memo,
        '0' AS rn,
        'native' AS transfer_type,
        amount_unadjusted :: STRING AS amount_raw,
        amount_unadjusted :: FLOAT AS amount_raw_precise,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        native_transfers
),
nep_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        contract_address,
        from_address,
        to_address,
        memo,
        rn :: STRING AS rn,
        'nep141' AS transfer_type,
        amount_unadjusted :: STRING AS amount_raw,
        amount_unadjusted :: FLOAT AS amount_raw_precise,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        nep_transfers
),
------------------------------   FINAL --------------------------------
transfer_union AS (
    SELECT
        *
    FROM
        nep_final
    UNION ALL
    SELECT
        *
    FROM
        native_final
),
FINAL AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        rn,
        contract_address,
        from_address,
        to_address,
        memo,
        amount_raw,
        amount_raw_precise,
        transfer_type,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        transfer_union
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','rn']
    ) }} AS transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL