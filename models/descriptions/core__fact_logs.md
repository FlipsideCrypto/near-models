{% docs core__fact_logs %}

## Description
This table contains all event logs emitted by smart contracts on the NEAR Protocol blockchain, capturing structured event data including token transfers, contract interactions, and custom application events. Logs represent the standardized way that smart contracts communicate state changes and events to external systems, following NEAR's event logging standards. This table provides the foundation for understanding contract behavior, token movements, and application-specific events across the NEAR ecosystem.

## Key Use Cases
- Token transfer tracking and balance analysis
- Smart contract event monitoring and analytics
- DeFi protocol activity analysis and volume tracking
- NFT minting and transfer event analysis
- Cross-contract communication tracking
- Application-specific event analysis and user behavior tracking
- Compliance and audit trail analysis

## Important Relationships
- Links to `core.fact_transactions` through tx_hash for transaction context
- Connects to `core.fact_receipts` through receipt_id for execution context
- Provides event data for `core.fact_token_transfers` and `core.ez_token_transfers`
- Supports `defi.ez_dex_swaps` and `defi.ez_bridge_activity` through event parsing
- Enables analysis in `nft.ez_nft_sales` through NFT-related events
- Powers `stats.ez_core_metrics_hourly` for aggregated event metrics

## Commonly-used Fields
- `log_index`: Essential for event ordering within transactions and receipts
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `receiver_id` and `predecessor_id`: Critical for contract interaction analysis
- `clean_log`: Important for event parsing and structured data analysis
- `event_standard`: Essential for identifying standardized event types
- `receipt_succeeded`: Important for filtering successful events only
- `gas_burnt`: Useful for gas consumption analysis and cost optimization

{% enddocs %} 