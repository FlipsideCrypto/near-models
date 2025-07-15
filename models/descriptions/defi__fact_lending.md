{% docs defi__fact_lending %}

## Description
This table contains all lending protocol transactions on the NEAR Protocol blockchain, primarily capturing activities on the Burrow lending protocol. The data includes lending actions such as deposits, withdrawals, borrows, and repayments, along with associated token amounts and protocol metadata. This table provides the foundation for lending analytics, protocol risk assessment, and DeFi lending market analysis across the NEAR ecosystem.

## Key Use Cases
- Lending protocol volume analysis and market activity monitoring
- Borrowing and lending pattern analysis and risk assessment
- Collateral utilization analysis and liquidation monitoring
- Interest rate impact analysis and yield optimization
- Protocol health monitoring and risk metrics calculation
- User behavior analysis in lending protocols
- Cross-protocol lending comparison and performance benchmarking

## Important Relationships
- Links to `core.fact_transactions` through tx_hash for transaction context
- Connects to `core.fact_blocks` through block_id for temporal context
- Provides lending data for `defi.ez_lending` with enhanced metadata
- Supports `defi.ez_intents` with lending intent analysis
- Enables analysis in `stats.ez_core_metrics_hourly` for lending metrics
- Powers cross-protocol analysis with other DeFi activities

## Commonly-used Fields
- `tx_hash`: Essential for linking to transaction details and verification
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `actions`: Critical for lending action classification and analysis
- `token_address`: Important for token-specific lending analysis
- `amount_raw` and `amount_adj`: Essential for lending volume calculations
- `sender_id`: Important for user behavior analysis and lending patterns
- `platform`: Critical for protocol-specific analysis and comparison

{% enddocs %} 