{% docs defi__fact_dex_swaps %}

## Description
This table contains all decentralized exchange (DEX) swap transactions on the NEAR Protocol blockchain, capturing token swaps across various DEX protocols including Ref Finance, Orderly Network, and other automated market makers. The data includes swap details such as input/output tokens, amounts, prices, and protocol-specific metadata. This table provides the foundation for DeFi analytics, trading volume analysis, and liquidity pool performance tracking across the NEAR ecosystem.

## Key Use Cases
- DEX trading volume analysis and market activity monitoring
- Token pair liquidity analysis and trading pattern identification
- Price impact analysis and slippage calculation
- DEX protocol comparison and performance benchmarking
- Arbitrage opportunity detection and analysis
- Trading bot activity monitoring and pattern recognition
- DeFi protocol integration analysis and cross-protocol flows

## Important Relationships
- Links to `core.fact_transactions` through tx_hash for transaction context
- Connects to `core.fact_blocks` through block_id for temporal context
- Provides swap data for `defi.ez_dex_swaps` with enhanced metadata and pricing
- Supports `defi.ez_intents` with swap execution analysis
- Enables analysis in `stats.ez_core_metrics_hourly` for DeFi metrics
- Powers cross-protocol analysis with other DeFi activities

## Commonly-used Fields
- `tx_hash`: Essential for linking to transaction details and verification
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `token_in` and `token_out`: Critical for token pair analysis and trading patterns
- `amount_in_raw` and `amount_out_raw`: Important for swap size analysis and volume calculations
- `receiver_id` and `signer_id`: Essential for user behavior analysis and trader identification
- `swap_index`: Important for multi-swap transaction analysis
- `swap_input_data`: Useful for detailed swap parameter analysis

{% enddocs %} 