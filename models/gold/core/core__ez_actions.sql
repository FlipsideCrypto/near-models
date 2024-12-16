{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    unique_key = 'actions_id',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,receipt_id,receipt_receiver_id,receipt_signer_id,receipt_predecessor_id);",
    tags = ['actions', 'curated', 'scheduled_core', 'grail']
) }}
-- depends_on: {{ ref('silver__streamline_transactions_final') }}
-- depends_on: {{ ref('silver__streamline_receipts_final') }}

{% if execute %}

    {% if is_incremental() and not var("MANUAL_FIX") %}
    {% do log("Incremental and not MANUAL_FIX", info=True) %}
    {% set max_mod_query %}

    SELECT
        MAX(modified_timestamp) modified_timestamp
    FROM
        {{ this }}

    {% endset %}

        {% set max_mod = run_query(max_mod_query) [0] [0] %}
        {% if not max_mod or max_mod == 'None' %}
            {% set max_mod = '2099-01-01' %}
        {% endif %}

        {% do log("max_mod: " ~ max_mod, info=True) %}

        {% set min_block_date_query %}
    SELECT
        MIN(
            block_timestamp :: DATE
        )
    FROM
        (
            SELECT
                MIN(block_timestamp) block_timestamp
            FROM
                {{ ref('silver__streamline_transactions_final') }} A
            WHERE
                modified_timestamp >= '{{max_mod}}'
            UNION ALL
            SELECT
                MIN(block_timestamp) block_timestamp
            FROM
                {{ ref('silver__streamline_receipts_final') }} A
            WHERE
                modified_timestamp >= '{{max_mod}}'
        ) 
    {% endset %}

        {% set min_bd = run_query(min_block_date_query) [0] [0] %}
        {% if not min_bd or min_bd == 'None' %}
            {% set min_bd = '2099-01-01' %}
        {% endif %}

        {% do log("min_bd: " ~ min_bd, info=True) %}

    {% endif %}

{% endif %}

WITH transactions AS (

    SELECT
        tx_hash,
        tx_signer,
        tx_receiver,
        gas_used AS tx_gas_used,
        tx_succeeded,
        transaction_fee AS tx_fee,
        modified_timestamp
    FROM
        {{ ref('silver__streamline_transactions_final') }}

    {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        {% if is_incremental() %}
            WHERE block_timestamp :: DATE >= '{{min_bd}}'
        {% endif %}
    {% endif %}
),
receipts AS (
    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_object_id AS receipt_id,
        receiver_id AS receipt_receiver_id,
        signer_id AS receipt_signer_id,
        receipt_actions :predecessor_id :: STRING AS receipt_predecessor_id,
        receipt_succeeded,
        gas_burnt AS receipt_gas_burnt,
        status_value,
        receipt_actions,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__streamline_receipts_final') }}

    {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        {% if is_incremental() %}
            WHERE block_timestamp :: DATE >= '{{min_bd}}'
        {% endif %}
    {% endif %}
),
join_data AS (
    SELECT
        r.block_id,
        r.block_timestamp,
        r.tx_hash,
        t.tx_signer,
        t.tx_receiver,
        t.tx_gas_used,
        t.tx_succeeded,
        t.tx_fee,
        r.receipt_id,
        r.receipt_receiver_id,
        r.receipt_signer_id,
        r.receipt_predecessor_id,
        r.receipt_succeeded,
        r.receipt_gas_burnt,
        r.status_value,
        r.receipt_actions,
        r._partition_by_block_number,
        r._inserted_timestamp
    FROM
        receipts r
        LEFT JOIN transactions t
        ON r.tx_hash = t.tx_hash

    {% if is_incremental() and not var("MANUAL_FIX") %}
        WHERE
            GREATEST(
                COALESCE(r.modified_timestamp, '1970-01-01'),
                COALESCE(t.modified_timestamp, '1970-01-01')   
            ) >= '{{max_mod}}'
    {% endif %}

),
flatten_actions AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        tx_signer,
        tx_receiver,
        tx_gas_used,
        tx_succeeded,
        tx_fee,
        receipt_id,
        receipt_receiver_id,
        receipt_signer_id,
        receipt_predecessor_id,
        receipt_succeeded,
        receipt_gas_burnt,
        IFF(
            object_keys(status_value)[0] :: STRING = 'SuccessValue', 
            OBJECT_INSERT(
                status_value, 
                'SuccessValue',
                COALESCE(
                    TRY_PARSE_JSON(
                        TRY_BASE64_DECODE_STRING(
                            GET(status_value, 'SuccessValue')
                        )
                    ), 
                    GET(status_value, 'SuccessValue')
                ),
                TRUE 
            ), 
            status_value
        ) as receipt_status_value,
        False AS is_delegated,
        INDEX AS action_index,
        receipt_actions :receipt :Action :gas_price :: NUMBER AS action_gas_price,
        IFF(
            VALUE = 'CreateAccount', 
            VALUE, 
            object_keys(VALUE) [0] :: STRING
        ) AS action_name,
        IFF(
            VALUE = 'CreateAccount',
            {},
            GET(VALUE, object_keys(VALUE) [0] :: STRING)
        ) AS action_data,
        MD5(
            CASE action_name
                WHEN 'FunctionCall' THEN
                    CONCAT_WS(',',
                    action_data :args :: STRING,
                    action_data :deposit :: STRING,
                    action_data :gas :: STRING,
                    action_data :method_name :: STRING
                )
                WHEN 'AddKey' THEN
                    CONCAT_WS(',',
                    action_data :access_key :nonce :: STRING,
                    action_data :access_key :permission :: STRING,
                    action_data :public_key :: STRING
                )
                WHEN 'DeleteKey' THEN
                    action_data :public_key :: STRING
                WHEN 'CreateAccount' THEN
                    'empty'  -- consistent hash for empty objects
                WHEN 'DeleteAccount' THEN
                    action_data :beneficiary_id :: STRING
                WHEN 'DeployContract' THEN
                    action_data :code :: STRING
                WHEN 'Transfer' THEN
                    action_data :deposit :: STRING
                WHEN 'Stake' THEN
                    CONCAT_WS(',',
                    action_data :public_key :: STRING,
                    action_data :stake :: STRING
                )
            ELSE
                -- Fallback: convert entire variant to string
                action_data :: STRING
            END
        ) AS action_hash,
        IFF(
            action_name = 'FunctionCall',
            OBJECT_INSERT(
                action_data,
                'args',
                COALESCE(
                    TRY_PARSE_JSON(
                        TRY_BASE64_DECODE_STRING(
                            action_data :args :: STRING
                        )
                    ),
                    action_data :args
                ),
                TRUE
            ),
            action_data
        ) AS action_data_parsed,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        join_data,
        LATERAL FLATTEN(
            receipt_actions :receipt :Action :actions :: ARRAY
        )
),
flatten_delegated_actions AS (
    SELECT
        tx_hash,
        True AS is_delegated,
        INDEX AS delegated_action_index,
        IFF(
            VALUE = 'CreateAccount', 
            VALUE, 
            object_keys(VALUE) [0] :: STRING
        ) AS delegated_action_name,
        IFF(
            VALUE = 'CreateAccount',
            {},
            GET(VALUE, object_keys(VALUE) [0] :: STRING)
        ) AS delegated_action_data,
        MD5(
            CASE delegated_action_name
                WHEN 'FunctionCall' THEN
                    CONCAT_WS(',',
                    delegated_action_data :args :: STRING,
                    delegated_action_data :deposit :: STRING,
                    delegated_action_data :gas :: STRING,
                    delegated_action_data :method_name :: STRING
                )
                WHEN 'AddKey' THEN
                    CONCAT_WS(',',
                    delegated_action_data :access_key :nonce :: STRING,
                    delegated_action_data :access_key :permission :: STRING,
                    delegated_action_data :public_key :: STRING
                )
                WHEN 'DeleteKey' THEN
                    delegated_action_data :public_key :: STRING
                WHEN 'CreateAccount' THEN
                    'empty'
                WHEN 'DeleteAccount' THEN
                    delegated_action_data :beneficiary_id :: STRING
                WHEN 'DeployContract' THEN
                    delegated_action_data :code :: STRING
                WHEN 'Transfer' THEN
                    delegated_action_data :deposit :: STRING
                WHEN 'Stake' THEN
                    CONCAT_WS(',',
                    delegated_action_data :public_key :: STRING,
                    delegated_action_data :stake :: STRING
                )
            ELSE
                -- Fallback: convert entire variant to string
                delegated_action_data :: STRING
            END
        ) AS delegated_action_hash
    FROM flatten_actions, LATERAL FLATTEN(action_data :delegate_action :actions :: ARRAY)
    WHERE action_name = 'Delegate'
)
SELECT
    block_id,
    block_timestamp,
    fa.tx_hash,
    tx_succeeded,
    tx_receiver,
    tx_signer,
    tx_gas_used,
    tx_fee,
    fa.receipt_id,
    receipt_predecessor_id,
    receipt_receiver_id,
    receipt_signer_id,
    receipt_succeeded,
    receipt_gas_burnt,
    receipt_status_value,
    action_index,
    COALESCE(
        da.is_delegated, 
        fa.is_delegated
    ) AS is_delegated,
    action_name,
    action_data_parsed AS action_data,
    action_gas_price,
    _partition_by_block_number,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id', 'action_index']
    ) }} AS actions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_actions fa
    LEFT JOIN flatten_delegated_actions da
    ON fa.tx_hash = da.tx_hash
    AND fa.action_name = da.delegated_action_name
    AND fa.action_hash = da.delegated_action_hash
    AND fa.action_index = da.delegated_action_index
