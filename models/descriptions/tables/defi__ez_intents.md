{% docs defi__ez_intents %}

## Description
This table provides an enhanced view of all intent-based transactions on the NEAR Protocol blockchain, combining raw intent data with token metadata, pricing information, and cross-chain mapping details. The table includes both NEP-245 and DIP-4 standard intents with decimal adjustments, USD values, and comprehensive token context.

**CRITICAL**: Each intent consists of multiple transaction steps (rows) representing the complete transfer flow: input (user → solver), output (solver → user), fees, and optional routing. Each unique intent is identified by `tx_hash` + `log_index`. Summing all `amount_usd` values will overcount volume by 2-3x because it counts input + output + fees separately.

## How to Use This Table

### Volume Calculation (Recommended Method)
**Use MAX per intent** to avoid 2-3x overcounting:

```sql
WITH intent_volumes AS (
    SELECT
        tx_hash,
        log_index,
        MAX(amount_usd) as intent_volume
    FROM near.defi.ez_intents
    WHERE amount_usd > 0.01  -- Exclude dust/fees
    GROUP BY tx_hash, log_index  -- BOTH fields required
)
SELECT SUM(intent_volume) as total_volume
FROM intent_volumes;
```

**Why MAX works**: Input ≈ output amounts (differ only by small fees). MAX captures the true swap size while ignoring dust.

### Common Query Patterns

**Daily volume by token:**
```sql
WITH intent_volumes AS (
    SELECT
        DATE_TRUNC('day', block_timestamp) as date,
        tx_hash, log_index, symbol,
        MAX(amount_usd) as volume
    FROM near.defi.ez_intents
    WHERE amount_usd > 0.01
    GROUP BY date, tx_hash, log_index, symbol
)
SELECT date, symbol, SUM(volume) as daily_volume
FROM intent_volumes
GROUP BY date, symbol;
```

**Cross-chain flows:**
```sql
WITH flows AS (
    SELECT
        tx_hash, log_index,
        MIN(blockchain) as source_chain,
        MAX(blockchain) as dest_chain,
        MAX(amount_usd) as volume
    FROM near.defi.ez_intents
    WHERE amount_usd > 0.01
    GROUP BY tx_hash, log_index
)
SELECT source_chain, dest_chain, SUM(volume) as total_volume
FROM flows
GROUP BY source_chain, dest_chain;
```

**Top users:**
```sql
WITH users AS (
    SELECT
        old_owner_id as user,
        tx_hash, log_index,
        MAX(amount_usd) as volume
    FROM near.defi.ez_intents
    WHERE amount_usd > 0.01
    GROUP BY user, tx_hash, log_index
)
SELECT user, SUM(volume) as total_volume
FROM users
GROUP BY user;
```

### Validation
Check if you're calculating correctly:
```sql
SELECT
    AVG(SUM(amount_usd) / NULLIF(MAX(amount_usd), 0)) as avg_ratio
FROM near.defi.ez_intents
WHERE amount_usd > 0.01
GROUP BY tx_hash, log_index;
```
**Expected**: ~1.5-2.0. If higher, you're likely overcounting.

## Key Use Cases
- Intent-based trading volume analysis with accurate calculations
- Cross-chain swap tracking and flow analysis
- User behavior analysis in intent-driven protocols
- Referral program effectiveness measurement
- Intent fulfillment rate analysis (use `receipt_succeeded`)
- MEV protection mechanism evaluation

## Important Relationships
- Enhances `defi.fact_intents` with pricing and metadata from `price.ez_prices_hourly`
- Combines token metadata from `core.dim_ft_contract_metadata` for complete token context
- Enables analysis in `stats.ez_core_metrics_hourly` for intent metrics
- Provides foundation for all intent-based analytics and reporting
- Powers cross-protocol analysis with other DeFi activities

## Commonly-used Fields
- `tx_hash` + `log_index`: **Unique intent identifier** - MUST group by both for accurate volume calculations
- `amount_usd`: Intent value in USD - use with MAX() per intent to avoid overcounting
- `symbol`: Token symbol for token-specific analysis (USDT, NEAR, ETH, etc.)
- `blockchain`: Source/destination chain (near, eth, bsc, sol, etc.) for cross-chain flow analysis
- `old_owner_id` / `new_owner_id`: Sender/recipient addresses - use `old_owner_id` to identify intent initiator
- `log_event_index`: Step order within intent - useful for understanding transaction flow
- `block_timestamp`: Time of transaction - use for time-series analysis and filtering
- `receipt_succeeded`: Transaction success status - filter to TRUE for successful intents only
- `referral`: Referral address for referral program analysis
- `fee_amount_usd`: Fee collected in USD - separate from main intent volume

{% enddocs %} 