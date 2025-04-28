{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'tx_hash',
    cluster_by = [ 'block_timestamp::date'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,lockup_account_id,owner_account_id);",
    tags = ['curated', 'scheduled_non_core'],
) }}

WITH
lockup_actions AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receipt_predecessor_id,
        receipt_receiver_id,
        receipt_signer_id,
        action_data,
        tx_succeeded,
        receipt_succeeded,
        _partition_by_block_number
    FROM
        {{ ref('core__ez_actions') }}
    WHERE 
        receipt_succeeded
        AND tx_succeeded
        AND action_name = 'FunctionCall'
        AND action_data :method_name :: STRING in (
            'on_lockup_create',
            'create',
            'new'
        )
        AND (
            receipt_signer_id = 'lockup.near' 
            OR receipt_predecessor_id = 'lockup.near'
            OR receipt_receiver_id ILIKE '%lockup.near'
        )

    {% if var("MANUAL_FIX") %}
      AND {{ partition_load_manual('no_buffer') }}
    {% else %}
            {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
    {% endif %}

),
xfers AS (
    SELECT
        tx_hash,
        action_id,
        block_timestamp,
        block_id,
        amount_unadj :: INT AS deposit,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        {{ ref('silver__token_transfer_native') }}

    {% if var("MANUAL_FIX") %}
      WHERE {{ partition_load_manual('no_buffer') }}
    {% else %}
            {% if is_incremental() %}
        WHERE modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
    {% endif %}
),
agg_arguments AS (
    SELECT
        tx_hash,
        OBJECT_AGG(
            action_data :method_name :: STRING,
            receipt_id :: variant
        ) AS receipt_ids,
        OBJECT_AGG(
            action_data :method_name :: STRING,
            receipt_receiver_id :: variant
        ) AS receiver_ids,
        MIN(block_id) AS block_id,
        MIN(block_timestamp) AS block_timestamp,
        OBJECT_AGG(
            action_data :method_name :: STRING,
            action_data :args :: variant
        ) AS args_all,
        COUNT(
            DISTINCT action_data :method_name :: STRING
        ) AS method_count,
        MIN(_partition_by_block_number) AS _partition_by_block_number
    FROM
        lockup_actions
    GROUP BY
        1
),
lockup_xfers AS (
    SELECT
        tx_hash,
        action_id,
        SPLIT(
            action_id,
            '-'
        ) [0] :: STRING AS receipt_id,
        block_timestamp,
        block_id,
        deposit,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        xfers
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                lockup_actions
        )
),
parse_args_json AS (
    SELECT
        A.tx_hash,
        receipt_ids,
        A.block_id,
        A.block_timestamp,
        COALESCE(
            args_all :on_lockup_create :attached_deposit :: DOUBLE,
            x.deposit
        ) AS deposit,
        COALESCE(
            args_all :on_lockup_create :lockup_account_id :: STRING,
            receiver_ids :new :: STRING
        ) AS lockup_account_id,
        COALESCE(
            args_all :new :owner_account_id :: STRING,
            args_all :create :owner_account_id :: STRING
        ) AS owner_account_id,
        COALESCE(
            args_all :new :lockup_duration :: STRING,
            args_all :create :lockup_duration :: STRING
        ) AS lockup_duration,
        COALESCE(
            args_all :new :lockup_timestamp :: STRING,
            args_all :create :lockup_timestamp :: STRING
        ) AS lockup_timestamp,
        COALESCE(
            args_all :new :release_duration :: STRING,
            args_all :create :release_duration :: STRING
        ) AS release_duration,
        COALESCE(
            args_all :new :vesting_schedule :: STRING,
            args_all :create :vesting_schedule :: STRING
        ) AS vesting_schedule,
        args_all :new :transfers_information :: STRING AS transfers_information,
        args_all,
        A._partition_by_block_number
    FROM
        agg_arguments A
        LEFT JOIN lockup_xfers x
        ON A.receipt_ids :new :: STRING = x.receipt_id
),
FINAL AS (
    SELECT
        tx_hash,
        receipt_ids AS receipt_object_ids, -- slated for rename to receipt_ids
        block_timestamp,
        block_id,
        deposit,
        lockup_account_id,
        owner_account_id,
        lockup_duration,
        lockup_timestamp,
        TO_TIMESTAMP_NTZ(lockup_timestamp) AS lockup_timestamp_ntz,
        release_duration,
        vesting_schedule,
        transfers_information,
        args_all,
        _partition_by_block_number
    FROM
        parse_args_json
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS lockup_actions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
