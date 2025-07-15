{% docs core__ez_actions %}

## Description
This table provides a comprehensive, action-level view of all blockchain activities on NEAR Protocol, combining transaction and receipt data to create detailed action records. Each record represents a specific action within a transaction, including function calls, token transfers, account creation, and other blockchain operations. The table normalizes complex receipt structures into standardized action records, enabling granular analysis of smart contract interactions, user behaviors, and protocol activities across the NEAR ecosystem.

## Key Use Cases
- Detailed smart contract interaction analysis and function call tracking
- Action-level user behavior analysis and pattern recognition
- Gas consumption analysis at the action level for optimization
- Cross-contract call tracking and dependency analysis
- Protocol-specific action monitoring and analytics
- DeFi protocol interaction analysis and volume tracking
- Account creation and management pattern analysis

## Important Relationships
- Combines data from `core.fact_transactions` and `core.fact_receipts` for comprehensive action analysis
- Provides detailed context for `core.fact_token_transfers` and `core.ez_token_transfers`
- Supports `defi.ez_dex_swaps` with detailed swap action analysis
- Enables `defi.ez_bridge_activity` with cross-chain action tracking
- Powers `defi.ez_intents` with intent-based action analysis
- Supports `stats.ez_core_metrics_hourly` with action-level metrics

## Commonly-used Fields
- `action_name`: Essential for categorizing and filtering specific action types
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `tx_hash` and `receipt_id`: Critical for linking to transaction and receipt context
- `action_data`: Important for detailed action parameter analysis
- `receipt_signer_id` and `receipt_receiver_id`: Essential for user and contract analysis
- `receipt_gas_burnt`: Important for gas consumption analysis and optimization
- `action_index`: Critical for action ordering within transactions

{% enddocs %} 