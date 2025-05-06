{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate","block_timestamp::date"],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    unique_key = 'block_id',
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(block_id, block_hash);",
    tags = ['scheduled_core'],
    full_refresh = false
) }}

WITH blocks AS (
    SELECT
        block_id,
        block_timestamp,
        block_hash,
        block_json :header :prev_hash :: STRING AS prev_hash,
        block_json :author :: STRING AS block_author,
        block_json :chunks :: ARRAY AS chunks_json,
        block_json :header :: OBJECT AS header_json,
        partition_key AS _partition_by_block_number
    FROM
        {{ ref('silver__blocks_v2') }}

        {% if is_incremental() %}
        WHERE
            modified_timestamp >= (
            SELECT
                COALESCE(MAX(modified_timestamp), '1970-01-01')
            FROM
                {{ this }}
            )
        {% endif %}
)
-- TODO: do I want to modify the schema at all?
-- review usage, can probably collapse several into the header
-- TODO: reduce duplication (header + extracted fields)
SELECT
    block_id,
    block_timestamp,
    block_hash,
    block_author,
    header_json AS header,
    header_json :challenges_result :: ARRAY AS block_challenges_result,
    header_json :challenges_root :: STRING AS block_challenges_root,
    header_json :chunk_headers_root :: STRING AS chunk_headers_root,
    header_json :chunk_tx_root :: STRING AS chunk_tx_root,
    header_json :chunk_mask :: ARRAY AS chunk_mask,
    header_json :chunk_receipts_root :: STRING AS chunk_receipts_root,
    chunks_json AS chunks,
    header_json :chunks_included :: NUMBER AS chunks_included,
    header_json :epoch_id :: STRING AS epoch_id,
    header_json :epoch_sync_data_hash :: STRING AS epoch_sync_data_hash,
    header_json :gas_price :: FLOAT AS gas_price,
    header_json :last_ds_final_block :: STRING AS last_ds_final_block,
    header_json :last_final_block :: STRING AS last_final_block,
    header_json :latest_protocol_version :: INT AS latest_protocol_version,
    header_json :next_bp_hash :: STRING AS next_bp_hash,
    header_json :next_epoch_id :: STRING AS next_epoch_id,
    header_json :outcome_root :: STRING AS outcome_root,
    prev_hash,
    header_json :prev_height :: NUMBER AS prev_height,
    header_json :prev_state_root :: STRING AS prev_state_root,
    header_json :random_value :: STRING AS random_value,
    header_json :rent_paid :: FLOAT AS rent_paid,
    header_json :signature :: STRING AS signature,
    header_json :total_supply :: FLOAT AS total_supply,
    header_json :validator_proposals :: ARRAY AS validator_proposals,
    header_json :validator_reward :: FLOAT AS validator_reward,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id']
    ) }} AS fact_blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    blocks
