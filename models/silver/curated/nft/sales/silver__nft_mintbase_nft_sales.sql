
WITH actions_events AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        signer_id,
        receiver_id,
        method_name,
        deposit,
        args,
        logs,
        attached_gas,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        receipt_succeeded = TRUE
        AND logs [0] IS NOT NULL

    {% if var("MANUAL_FIX") %}
      AND {{ partition_load_manual('no_buffer') }}
    {% else %}
        {% if is_incremental() %}
        AND _modified_timestamp >= (
            SELECT
                MAX(_modified_timestamp)
            FROM
                {{ this }}
        )
        {% endif %}
    {% endif %}
),
raw_logs AS (
    SELECT
        *,
        l.index AS logs_index,
        TRY_PARSE_JSON(REPLACE(l.value :: STRING, 'EVENT_JSON:', '')) AS event_json
    FROM
        actions_events A,
        LATERAL FLATTEN(
            input => A.logs
        ) l
),
mintbase_nft_sales AS (
    SELECT
        action_id,
        block_id,
        block_timestamp,
        tx_hash,
        attached_gas,
        IFF(method_name = 'buy', TRUE, FALSE) AS is_buy,  --- else resolve_nft_payout
        IFF(is_buy, args :nft_contract_id, args :token :owner_id) :: STRING AS seller_address,
        IFF( is_buy, signer_id, args :token :current_offer :from) :: STRING AS buyer_address,
        receiver_id AS platform_address,
        'Mintbase' AS platform_name,
        IFF(is_buy, args :nft_contract_id, args :token :store_id) :: STRING AS nft_address,
        IFF(is_buy, args :token_id, args :token :id) :: STRING AS token_id,
        IFF(is_buy, deposit, args :token :current_offer :price) / 1e24 AS price,
        IFF(is_buy, 'nft_sale', 'nft_sold') AS method_name,
        args AS LOG,
        event_json,
        logs_index,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        raw_logs
    WHERE
        event_json:data:standard = 'mb_market' and event_json:data:event = 'nft_sale'
)
SELECT
    *
FROM
    mintbase_nft_sales
LIMIT 10