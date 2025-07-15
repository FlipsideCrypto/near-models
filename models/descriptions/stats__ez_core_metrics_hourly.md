{% docs stats__ez_core_metrics_hourly %}

## Description
This table provides comprehensive hourly metrics for the NEAR Protocol blockchain, combining block production, transaction activity, and economic data with USD pricing information. The table includes block counts, transaction volumes, success rates, unique user counts, and fee calculations in both native NEAR and USD terms. This easy view eliminates the need for complex joins and calculations, providing ready-to-use data for network health monitoring, performance analysis, and economic assessment across the NEAR ecosystem.

## Key Use Cases
- Network health monitoring and performance analysis
- Transaction volume analysis and success rate tracking
- Economic analysis with USD value calculations
- User activity analysis and adoption tracking
- Network fee analysis and economic impact assessment
- Cross-hour performance comparison and trend identification
- Network capacity analysis and scaling assessment

## Important Relationships
- Combines data from `stats.core_metrics_block_hourly` and `stats.core_metrics_hourly` for comprehensive metrics
- Enhances metrics with pricing data from `price.ez_prices_hourly` for USD calculations
- Provides aggregated metrics for network health monitoring and reporting
- Supports all other gold models with network-level context and metrics
- Powers dashboard analytics and network performance reporting
- Enables cross-protocol comparison and benchmarking

## Commonly-used Fields
- `block_timestamp_hour`: Essential for time-series analysis and trend detection
- `transaction_count` and `transaction_count_success`: Critical for network activity analysis
- `total_fees_native` and `total_fees_usd`: Important for economic analysis and fee impact assessment
- `unique_from_count` and `unique_to_count`: Essential for user activity analysis and adoption tracking
- `block_count`: Important for network throughput and capacity analysis
- `block_number_min` and `block_number_max`: Useful for block range analysis and completeness verification

{% enddocs %} 