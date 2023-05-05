{{ config(
    materialized = 'incremental',
    unique_key = 'tx_hash'
) }}

WITH lockup_actions AS (

    SELECT
        *
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        receiver_id = 'lockup.near'
        AND method_name IN (
            'on_lockup_create',
            'create'
        )
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            AND {{ partition_load_manual('no_buffer') }}
        {% else %}
            AND {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}

),
method_on_lockup_create AS (
    SELECT
        tx_hash,
        block_timestamp,
        signer_id,
        args,
        args :attached_deposit :: DOUBLE * 1e-24 AS deposit,
        args :lockup_account_id :: STRING AS lockup_account_id,
        _load_timestamp,
        _partition_by_block_number
    FROM
        lockup_actions
    WHERE
        method_name = 'on_lockup_create'
),
method_create AS (
    SELECT
        tx_hash,
        block_timestamp,
        signer_id,
        deposit * 1e-24 AS deposit,
        args,
        args :owner_account_id :: STRING AS owner_account_id,
        args :lockup_duration :: STRING AS lockup_duration,
        args :lockup_timestamp AS lockup_timestamp,
        TO_TIMESTAMP_NTZ(lockup_timestamp) AS lockup_timestamp_ntz,
        args :vesting_schedule :: STRING AS vesting_schedule,
        args :release_duration :: STRING AS release_duration,
        _load_timestamp,
        _partition_by_block_number
    FROM
        lockup_actions
    WHERE
        method_name = 'create'
),
FINAL AS (
    SELECT
        olc.tx_hash,
        olc.block_timestamp,
        olc.signer_id,
        C.deposit,
        olc.lockup_account_id,
        C.owner_account_id,
        C.lockup_duration,
        C.lockup_timestamp,
        C.lockup_timestamp_ntz,
        C.release_duration,
        C.vesting_schedule,
        olc.args AS olc_args,
        C.args AS c_args,
        _load_timestamp,
        _partition_by_block_number
    FROM
        method_on_lockup_create olc
        LEFT JOIN method_create C USING (tx_hash)
)
SELECT
    *
FROM
    FINAL
