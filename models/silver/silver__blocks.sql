{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
) }}

WITH base_blocks AS (

    SELECT
        record_id,
        offset_id,
        block_id,
        block_timestamp,
        network,
        chain_id,
        tx_count,
        header,
        _ingested_at,
        _inserted_timestamp
    FROM
        {{ ref('bronze__blocks') }}
    WHERE
        {{ incremental_load_filter('_inserted_timestamp') }}
        qualify ROW_NUMBER() over (
            PARTITION BY block_id
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
),
FINAL AS (
    SELECT
        block_id,
        block_timestamp,
        header :hash :: STRING AS block_hash,
        header :tx_count :: INTEGER AS tx_count,
        header :author :: STRING AS block_author,
        header :challenges_result AS block_challenges_result,
        header :challenges_root :: STRING AS block_challenges_root,
        header :chunk_headers_root :: STRING AS chunk_headers_root,
        header :chunk_mask AS chunk_mask,
        header :chunk_receipts_root :: STRING AS chunk_receipts_root,
        header :chunk_tx_root :: STRING AS chunk_tx_root,
        header :chunks AS chunks,
        header :chunks_included :: INTEGER AS chunks_included,
        header :epoch_id :: STRING AS epoch_id,
        header :epoch_sync_data_hash :: STRING AS epoch_sync_data_hash,
        header :events AS events,
        header :gas_price :: FLOAT AS gas_price,
        header :last_ds_final_block :: STRING AS last_ds_final_block,
        header :last_final_block :: STRING AS last_final_block,
        header :latest_protocol_version :: INTEGER AS latest_protocol_version,
        header :next_bp_hash :: STRING AS next_bp_hash,
        header :next_epoch_id :: STRING AS next_epoch_id,
        header :outcome_root :: STRING AS outcome_root,
        header :prev_hash :: STRING AS prev_hash,
        header :prev_height :: INTEGER AS prev_height,
        header :prev_state_root :: STRING AS prev_state_root,
        header :random_value :: STRING AS random_value,
        header :rent_paid :: FLOAT AS rent_paid,
        header :signature :: STRING AS signature,
        header :total_supply :: FLOAT AS total_supply,
        header :validator_proposals AS validator_proposals,
        header :validator_reward :: FLOAT AS validator_reward,
        _ingested_at,
        _inserted_timestamp
    FROM
        base_blocks
)
SELECT
    *
FROM
    FINAL
