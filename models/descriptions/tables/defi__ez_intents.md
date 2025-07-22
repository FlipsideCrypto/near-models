{% docs defi__ez_intents %}

## Description
This table provides an enhanced view of all intent-based transactions on the NEAR Protocol blockchain, combining raw intent data with token metadata, pricing information, and cross-chain mapping details. The table includes both NEP-245 and DIP-4 standard intents with decimal adjustments, USD values, and comprehensive token context. This easy view eliminates the need for complex joins and calculations, providing ready-to-use data for intent-based trading analysis and MEV protection mechanism evaluation.

## Key Use Cases
- Intent-based trading analysis with accurate decimal-adjusted amounts and USD values
- MEV protection mechanism effectiveness analysis with economic impact
- User behavior analysis in intent-driven protocols with value context
- Cross-protocol intent execution tracking and performance comparison
- Intent fulfillment rate analysis and optimization with value calculations
- Referral program analysis and user acquisition tracking
- Intent-based DeFi protocol performance monitoring and benchmarking

## Important Relationships
- Enhances `defi.fact_intents` with pricing and metadata from `price.ez_prices_hourly`
- Combines token metadata from `core.dim_ft_contract_metadata` for complete token context
- Enables analysis in `stats.ez_core_metrics_hourly` for intent metrics
- Provides foundation for all intent-based analytics and reporting
- Powers cross-protocol analysis with other DeFi activities

## Commonly-used Fields
- `amount` and `amount_usd`: Essential for accurate intent value analysis
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `log_event`: Critical for intent type classification and analysis
- `owner_id`: Important for user behavior analysis and intent tracking
- `token_id` and `symbol`: Essential for token-specific intent analysis
- `referral`: Important for referral program analysis and user acquisition
- `receipt_succeeded`: Critical for intent fulfillment rate analysis

{% enddocs %} 