{{ config(
    materialized = 'incremental',
    unique_key = 'tx_hash',
    tags = ['curated'],
) }}

WITH txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_transactions_final') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        WHERE
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),
function_calls AS (
    SELECT
        *
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        WHERE
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),
xfers AS (
    SELECT
        *
    FROM
        {{ ref('silver__transfers_s3') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        WHERE
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),
lockup_actions AS (
    SELECT
        tx_hash,
        action_id,
        SPLIT(
            action_id,
            '-'
        ) [0] :: STRING AS receipt_object_id,
        block_timestamp,
        block_id,
        signer_id,
        receiver_id,
        args,
        deposit,
        method_name,
        _load_timestamp,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        function_calls
    WHERE
        (
            signer_id = 'lockup.near'
            OR receiver_id = 'lockup.near'
            OR receiver_id ILIKE '%lockup.near'
        )
        AND method_name IN (
            'on_lockup_create',
            'create',
            'new'
        )
        AND tx_hash NOT IN (
            'Ez6rNL3fP62c4nMroYUmjVR4MbqEeVoL6RzmuajGQrkS',
            'TcCm1jzMFnwgAT3Wh2Qr1n2tR7ZVXKcv3ThKbXAhe7H',
            'm3mf5maDHfD2MXjvRnxp38ZA5BAVnRumiebPEqjGmuC'
        )
),
agg_arguments AS (
    SELECT
        tx_hash,
        OBJECT_AGG(
            method_name,
            receipt_object_id :: variant
        ) AS receipt_object_ids,
        OBJECT_AGG(
            method_name,
            receiver_id :: variant
        ) AS receiver_ids,
        MIN(block_id) AS block_id,
        MIN(block_timestamp) AS block_timestamp,
        OBJECT_AGG(
            method_name,
            args
        ) AS args_all,
        COUNT(
            DISTINCT method_name
        ) AS method_count,
        MIN(_load_timestamp) AS _load_timestamp,
        MIN(_partition_by_block_number) AS _partition_by_block_number,
        MIN(_inserted_timestamp) AS _inserted_timestamp
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
        ) [0] :: STRING AS receipt_object_id,
        block_timestamp,
        block_id,
        deposit,
        _load_timestamp,
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
        receipt_object_ids,
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
        A._load_timestamp,
        A._partition_by_block_number,
        A._inserted_timestamp
    FROM
        agg_arguments A
        LEFT JOIN lockup_xfers x
        ON A.receipt_object_ids :new :: STRING = x.receipt_object_id
),
FINAL AS (
    SELECT
        f.tx_hash,
        f.receipt_object_ids,
        f.block_timestamp,
        f.block_id,
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
        f._load_timestamp,
        f._partition_by_block_number,
        f._inserted_timestamp
    FROM
        parse_args_json f
        LEFT JOIN txs USING (tx_hash)
    WHERE
        tx_status != 'Fail'
)
SELECT
    *
FROM
    FINAL
