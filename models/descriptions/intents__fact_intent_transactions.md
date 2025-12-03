{% docs intents__fact_intent_transactions %}

## Description
This table provides granular transaction-level data for all intent executions on the NEAR blockchain using the NEP245 (multi-token) standard combined with DIP4 (Decentralized Intent Protocol v4) metadata. Each row represents a single token transfer, mint, or burn operation within an intent execution, capturing the complete lifecycle of multi-token movements through the intent protocol. The table combines flattened NEP245 event data with enriched DIP4 metadata including referral information and fee collection details. This fact table serves as the foundational source for intent transaction analysis, providing the raw event data that powers higher-level analytics and easy views.

## Key Use Cases
- Token-level intent execution analysis and tracking individual token movements
- Intent protocol volume analysis by counting distinct transactions and measuring token flow
- Referral program attribution and effectiveness measurement
- Fee revenue analysis when combined with fees_collected_raw metadata
- Multi-token bundle analysis for complex intent executions involving multiple assets
- Transaction success rate analysis using receipt_succeeded for operational metrics
- Intent execution pattern analysis by examining log_event types (mt_transfer, mt_mint, mt_burn)

## Important Relationships
- Sources from `silver.logs_nep245` for flattened NEP245 multi-token event data
- Enriched with `silver.logs_dip4` for referral tracking and fee collection metadata
- Parent table for `intents.ez_intents` which provides user-friendly aggregated intent views
- Parent table for `intents.ez_fees` which extracts and analyzes fee collection details
- Can be joined with `core.fact_transactions` using tx_hash for complete transaction context
- Can be joined with `core.fact_receipts` using receipt_id for receipt execution details
- Related to `core.dim_ft_contract_metadata` via token_id for token metadata enrichment

## Commonly-used Fields
- `block_timestamp`: Essential for time-series analysis and temporal pattern identification
- `tx_hash` and `receipt_id`: Primary keys for joining with core transaction and receipt tables
- `token_id`: Critical for identifying which multi-token assets are being transferred
- `amount_raw`: Core field for volume analysis, must be decimal-adjusted using token metadata
- `log_event`: Key field for filtering by operation type (mt_transfer, mt_mint, mt_burn)
- `owner_id`, `old_owner_id`, `new_owner_id`: Critical for tracking token custody and building account balances
- `referral`: Important for attributing transactions to referral sources and partners
- `fees_collected_raw`: Used in revenue analysis and fee structure research
- `receipt_succeeded`: Essential filter for excluding failed transactions from analytics

{% enddocs %}
