{% docs intents__ez_transactions %}

## Description
This table provides enriched, analytics-ready intent transaction data with complete token metadata, USD pricing, and fee information. Built on top of the fact_transactions table, this easy view combines raw intent execution data with token labels from crosschain metadata and real-time price feeds to deliver human-readable transaction values. Each row represents a single token transfer, mint, or burn operation within an intent execution, enhanced with decimal-adjusted amounts, USD valuations, and comprehensive fee details. This is the primary table for intent protocol analytics, abstracting away the complexity of raw blockchain data and token standards to provide a clean, queryable interface for business intelligence and data analysis.

## Key Use Cases
- Intent protocol volume and revenue analysis with USD-normalized values
- Token flow analysis across different assets and blockchains
- Fee revenue tracking and referral program attribution with USD calculations
- User behavior analysis through intent transaction patterns
- Cross-chain token movement analysis using enriched blockchain metadata
- Protocol health monitoring via transaction success rates and gas consumption
- Multi-token bundle analysis for complex intent executions
- Comparative analytics across different token types and standards

## Important Relationships
- Sources from `intents.fact_transactions` for base intent transaction data
- Enriched with `silver.ft_contract_metadata` for token labels, symbols, and decimals
- Joins with `crosschain_price.ez_prices_hourly` for USD price data via ASOF joins
- Can be aggregated to create protocol-level metrics and dashboards
- Related to `intents.ez_fees` which provides detailed fee-specific analytics
- Can be joined with `core.ez_token_transfers` using tx_hash for broader token context
- Can be joined with `core.fact_transactions` for complete transaction details

## Commonly-used Fields
- `block_timestamp`: Critical for time-series analysis and temporal patterns
- `tx_hash` and `receipt_id`: Essential for transaction-level joins and debugging
- `amount_adj`: Decimal-adjusted amount, the primary field for volume analysis
- `amount_usd`: USD value of transfers, essential for financial analysis and reporting
- `symbol`: Human-readable token identifier for grouping and filtering
- `log_event`: Key filter field for operation type (mt_transfer, mt_mint, mt_burn)
- `referral`: Important for attribution analysis and partner tracking
- `fee_amount_usd`: Critical for protocol revenue analysis
- `receipt_succeeded`: Essential filter to exclude failed transactions
- `token_is_verified`: Quality flag for filtering to verified tokens only

{% enddocs %}
