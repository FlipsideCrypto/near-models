{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE', '_partition_by_block_number'],
    unique_key = 'block_id',
    tags = ['load', 'load_blocks','scheduled_core'],
    full_refresh = False
) }}

{% if execute and var('MANUAL_FIX') %}
-- NOTE "STREAMLINE_LOAD_LOOKBACK_HOURS" in metadata needs to be set as well
-- or _inserted_timestamp will be null

    {% set missing_blocks %}
        SELECT
            min(floor(prior_block_id, -3)) as min_partition,
            max(floor(block_id, -3)) as max_partition,
            min(prior_block_id) as min_block_id,
            max(block_id) as max_block_id
        FROM near.tests.block_gaps
    {% endset %}

    {% set missing_blocks_result = run_query(missing_blocks) %}
    {% set min_partition = missing_blocks_result[0][0] %}
    {% set max_partition = missing_blocks_result[0][1] %}
    {% set min_block_id = missing_blocks_result[0][2] %}
    {% set max_block_id = missing_blocks_result[0][3] %}
{% endif %}

WITH external_blocks AS (

    SELECT
        metadata$filename AS _filename,
        VALUE,
        _partition_by_block_number
    FROM
        {{ source(
            "streamline",
            "blocks"
        ) }}
    {% if var('MANUAL_FIX') %}
        WHERE
            _partition_by_block_number BETWEEN {{ min_partition }} AND {{ max_partition }}
    {% else %}
        WHERE
            _partition_by_block_number >= (
                SELECT
                    MAX(_partition_by_block_number) - (3000 * {{ var('STREAMLINE_LOAD_LOOKBACK_HOURS') }})
                FROM
                    {{ this }}
            )
    {% endif %}
),
meta AS (
    SELECT
        job_created_time AS _inserted_timestamp,
        file_name AS _filename
    FROM
        TABLE(
            information_schema.external_table_file_registration_history(
                start_time => DATEADD(
                    'hour', 
                    -{{ var('STREAMLINE_LOAD_LOOKBACK_HOURS') }},
                    SYSDATE()
                ),
                table_name => '{{ source( 'streamline', 'blocks' ) }}'
            )
        ) A
),
blocks AS (
    SELECT
        e.value :header :height :: NUMBER AS block_id,
        TO_TIMESTAMP_NTZ(
            e.value :header :timestamp :: STRING
        ) AS block_timestamp,
        e.value :header :hash :: STRING AS block_hash,
        e.value :header :prev_hash :: STRING AS prev_hash,
        e.value :author :: STRING AS block_author,
        e.value :header :gas_price :: NUMBER AS gas_price,
        e.value :header :total_supply :: NUMBER AS total_supply,
        e.value :header :validator_proposals :: ARRAY AS validator_proposals,
        e.value :header :validator_reward :: NUMBER AS validator_reward,
        e.value :header :latest_protocol_version :: NUMBER AS latest_protocol_version,
        e.value :header :epoch_id :: STRING AS epoch_id,
        e.value :header :next_epoch_id :: STRING AS next_epoch_id,
        NULL AS tx_count,
        -- tx_count is legacy field, deprecate from core view
        [] AS events,
        -- events does not exist, Figment created this
        e.value :chunks :: ARRAY AS chunks,
        e.value :header :: OBJECT AS header,
        e._partition_by_block_number,
        m._inserted_timestamp
    FROM
        external_blocks e
        LEFT JOIN meta m USING (
            _filename
        )
        
    {% if var('MANUAL_FIX') %}
        WHERE
            block_id BETWEEN {{ min_block_id }} AND {{ max_block_id }}
    {% else%}
        {% if is_incremental() %}
            WHERE
                _inserted_timestamp >= (
                    SELECT
                        MAX(_inserted_timestamp)
                    FROM
                        {{ this }}
                )
        {% endif %}
    {% endif %}
)
SELECT
    block_id,
    block_timestamp,
    block_hash,
    prev_hash,
    block_author,
    gas_price,
    total_supply,
    validator_proposals,
    validator_reward,
    latest_protocol_version,
    epoch_id,
    next_epoch_id,
    tx_count,
    events,
    chunks,
    header,
    _partition_by_block_number,
    {% if var('MANUAL_FIX') %}COALESCE(_inserted_timestamp, SYSDATE()) AS _inserted_timestamp,{% else %} _inserted_timestamp,{% endif %}
    {{ dbt_utils.generate_surrogate_key(
        ['block_id']
    ) }} AS streamline_blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    blocks 
    qualify ROW_NUMBER() over (
        PARTITION BY block_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
