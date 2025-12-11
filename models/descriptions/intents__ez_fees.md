{% docs intents__ez_fees %}

## Description
This table tracks all fees collected from intent executions on the NEAR blockchain using the DIP4 (Decentralized Intent Protocol v4) standard. The data captures fee collection events from intent protocol interactions, providing a detailed view of protocol revenue by extracting and flattening fees from DIP4 event logs. Each row represents a single fee payment in a specific token, with complete token metadata, pricing information, and execution context. The model enriches raw fee data with token labels from crosschain metadata and USD valuations from price feeds, enabling comprehensive fee analysis across different tokens and time periods.

## Key Use Cases
- Intent protocol revenue analysis and fee tracking over time
- Fee token distribution analysis to understand which tokens are preferred for fee payments
- Cost analysis for intent execution from the user perspective
- Protocol economics research comparing fee structures across different intent implementations
- Referral program analysis by tracking fee attribution to referral accounts
- Cross-chain fee analysis for bridged tokens used in intent executions

## Important Relationships
- Sources fee events from `silver.logs_dip4` which parses DIP4 standard event logs
- Joins with `core.dim_ft_contract_metadata` to enrich fee tokens with blockchain, symbol, and decimal information
- Uses `price.ez_prices_hourly` to calculate USD values for fees at execution time
- Related to `intents.ez_intents` which provides the broader intent execution context including transfers and outcomes
- Can be joined with `core.fact_transactions` using tx_hash for complete transaction details
- Can be joined with `core.fact_receipts` using receipt_id for receipt execution details

## Commonly-used Fields
- `block_timestamp`: Essential for time-series analysis of fee trends and protocol revenue over time
- `tx_hash` and `receipt_id`: Critical for linking to transaction and receipt details for complete execution context
- `intent_hash`: Key field for correlating fees with specific intent executions across tables
- `account_id`: Important for analyzing which accounts are generating the most fee volume
- `fee_symbol` and `fee_amount_usd`: Core fields for revenue analysis and understanding fee token preferences
- `fee_amount_adj`: Human-readable fee amount used in most analytics after decimal adjustment
- `referral`: Essential for tracking referral program effectiveness and fee attribution
- `receipt_succeeded`: Critical filter to analyze only successful fee collections vs failed attempts

{% enddocs %}
