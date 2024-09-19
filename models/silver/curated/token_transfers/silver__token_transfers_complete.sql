{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::Date'],
    unique_key = 'transfers_complete_id',
    incremental_strategy = 'merge',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,contract_address,from_address,to_address);",
    tags = ['curated','scheduled_non_core']
) }}

WITH native_transfers AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        '0' AS rn,
        'wrap.near' AS contract_address,
        predecessor_id AS from_address,
        receiver_id AS to_address,
        NULL AS memo,
        IFF(REGEXP_LIKE(deposit, '^[0-9]+$'), deposit, NULL) AS amount_unadj,
        'native' AS transfer_type,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__transfers_s3') }}
    WHERE
        status = TRUE AND deposit != 0
        {% if var("MANUAL_FIX") %}
        AND
            {{ partition_load_manual('no_buffer') }}

            {% elif is_incremental() %}
        AND
            _modified_timestamp >= (
                SELECT
                    MAX(_modified_timestamp)
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
        rn,
        contract_address,
        from_address,
        to_address,
        memo,
        amount_unadj,
        'nep141' AS transfer_type,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__token_transfer_ft_transfers_method') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}

            {% elif is_incremental() %}
        WHERE
            _modified_timestamp >= (
                SELECT
                    MAX(_modified_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
),
ft_transfers_event AS (
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
        amount_unadj,
        'nep141' AS transfer_type,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__token_transfer_ft_transfers_event') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}

            {% elif is_incremental() %}
        WHERE
            _modified_timestamp >= (
                SELECT
                    MAX(_modified_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
),
mints AS (
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
        amount_unadj,
        'nep141' AS transfer_type,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__token_transfer_mints') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}

            {% elif is_incremental() %}
        WHERE
            _modified_timestamp >= (
                SELECT
                    MAX(_modified_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
),
orders AS (
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
        amount_unadj,
        'nep141' AS transfer_type,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__token_transfer_orders') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}

            {% elif is_incremental() %}
        WHERE
            _modified_timestamp >= (
                SELECT
                    MAX(_modified_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
),
liquidity AS (
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
        amount_unadj,
        'nep141' AS transfer_type,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__token_transfer_liquidity') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}

            {% elif is_incremental() %}
        WHERE
            _modified_timestamp >= (
                SELECT
                    MAX(_modified_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
),
FINAL AS (
    SELECT
        *
    FROM
        native_transfers
    UNION ALL
    SELECT
        *
    FROM
        ft_transfers_method
    UNION ALL
    SELECT
        *
    FROM
        ft_transfers_event
    UNION ALL
    SELECT
        *
    FROM
        mints
    UNION ALL
    SELECT
        *
    FROM
        orders
    UNION ALL
    SELECT
        *
    FROM
        liquidity
)
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
    amount_unadj,
    transfer_type,
    _inserted_timestamp,
    _modified_timestamp,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['action_id','contract_address','amount_unadj','from_address','to_address','rn']
    ) }} AS transfers_complete_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
