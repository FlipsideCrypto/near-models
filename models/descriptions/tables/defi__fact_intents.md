{% docs defi__fact_intents %}

## Description
This table contains all intent-based transactions on the NEAR Protocol blockchain, capturing user intents for token transfers, swaps, and other DeFi operations through the intents.near protocol. The data includes both NEP-245 and DIP-4 standard intents, providing comprehensive tracking of intent creation, execution, and fulfillment. This table enables analysis of intent-based trading patterns, MEV protection mechanisms, and user behavior in intent-driven DeFi protocols.

## Key Use Cases
- Intent-based trading analysis and pattern recognition
- MEV protection mechanism effectiveness analysis
- User behavior analysis in intent-driven protocols
- Cross-protocol intent execution tracking
- Intent fulfillment rate analysis and optimization
- Referral program analysis and user acquisition tracking
- Intent-based DeFi protocol performance monitoring

## Important Relationships
- Links to `core.fact_transactions` through tx_hash for transaction context
- Connects to `core.fact_blocks` through block_id for temporal context
- Provides intent data for `defi.ez_intents` with enhanced metadata
- Supports `defi.ez_dex_swaps` with intent execution analysis
- Enables analysis in `stats.ez_core_metrics_hourly` for intent metrics
- Powers cross-protocol analysis with other DeFi activities

## Commonly-used Fields
- `tx_hash`: Essential for linking to transaction details and verification
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `log_event`: Critical for intent type classification and analysis
- `owner_id`: Important for user behavior analysis and intent tracking
- `amount_raw` and `token_id`: Essential for intent value analysis
- `referral`: Important for referral program analysis and user acquisition
- `receipt_succeeded`: Critical for intent fulfillment rate analysis

{% enddocs %} 