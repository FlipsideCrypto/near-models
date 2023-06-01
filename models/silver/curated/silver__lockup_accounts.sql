{{ config(
    materialized = 'table',
    unique_key = 'lockup_account_id',
    cluster_by = 'lockup_account_id',
    tags = ['curated']
) }}

WITH lockup_actions AS (

    SELECT
        *
    FROM
        {{ ref('silver__lockup_actions') }}
),
actions_events AS (
    SELECT
        *
    FROM
        {{ ref('silver__actions_events_s3') }}
),
all_lockup_accounts AS (
    SELECT
        lockup_account_id,
        MIN(DATE_TRUNC('day', block_timestamp)) AS creation_date
    FROM
        lockup_actions
    GROUP BY
        1
),
deletion_date AS (
    SELECT
        signer_id AS lockup_account_id,
        DATE_TRUNC(
            'day',
            block_timestamp
        ) AS deletion_date,
        block_id,
        action_data :beneficiary_id :: STRING AS beneficiary_id,
        FALSE AS is_active
    FROM
        actions_events
    WHERE
        signer_id = receiver_id
        AND signer_id LIKE '%.lockup.near'
        AND action_name = 'DeleteAccount'
),
FINAL AS (
    SELECT
        A.lockup_account_id,
        A.creation_date,
        deletion_date,
        block_id as deletion_block_id,
        beneficiary_id,
        COALESCE(
            is_active,
            TRUE
        ) AS is_active
    FROM
        all_lockup_accounts A
        LEFT JOIN deletion_date d USING (lockup_account_id)
)
SELECT
    *
FROM
    FINAL
