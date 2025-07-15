{% docs nft__fact_nft_transfers %}

## Description
This table contains all NFT transfer transactions on the NEAR Protocol blockchain, capturing the movement of NFTs between accounts and contracts. The data includes transfer details such as sender and receiver addresses, token IDs, contract addresses, and associated metadata. This table provides the foundation for NFT ownership tracking, transfer pattern analysis, and marketplace activity monitoring across the NEAR ecosystem.

## Key Use Cases
- NFT ownership tracking and transfer history analysis
- NFT marketplace activity monitoring and volume analysis
- NFT transfer pattern analysis and user behavior tracking
- Cross-collection transfer comparison and performance analysis
- NFT liquidity analysis and trading pattern identification
- NFT holder analysis and distribution tracking
- NFT ecosystem health monitoring and activity assessment

## Important Relationships
- Links to `core.fact_transactions` through tx_hash for transaction context
- Connects to `core.fact_blocks` through block_id for temporal context
- Provides transfer data for `nft.ez_nft_sales` with enhanced metadata
- Supports `nft.fact_nft_mints` with transfer context
- Enables analysis in `stats.ez_core_metrics_hourly` for NFT metrics
- Powers cross-collection analysis and ecosystem mapping

## Commonly-used Fields
- `tx_hash`: Essential for linking to transaction details and verification
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `token_id`: Critical for unique NFT identification and tracking
- `from_address` and `to_address`: Important for ownership analysis and flow tracking
- `contract_address`: Essential for collection identification and contract analysis
- `action_id`: Important for action-level analysis and receipt context
- `block_id`: Useful for temporal ordering and block-level analysis

{% enddocs %} 