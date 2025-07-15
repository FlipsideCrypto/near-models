{% docs nft__fact_nft_mints %}

## Description
This table contains all NFT minting transactions on the NEAR Protocol blockchain, capturing the creation of new NFTs across various collections and standards. The data includes minting details such as token IDs, minting methods, arguments, deposits, and associated metadata. This table provides the foundation for NFT analytics, collection growth tracking, and minting pattern analysis across the NEAR ecosystem.

## Key Use Cases
- NFT collection growth analysis and minting velocity tracking
- NFT minting pattern analysis and user behavior monitoring
- Collection launch analysis and initial distribution tracking
- NFT minting cost analysis and gas optimization
- Cross-collection minting comparison and performance benchmarking
- NFT creator analysis and minting strategy evaluation
- NFT ecosystem health monitoring and collection discovery

## Important Relationships
- Links to `core.fact_transactions` through tx_hash for transaction context
- Connects to `core.fact_blocks` through block_id for temporal context
- Provides minting data for `nft.ez_nft_sales` with enhanced metadata
- Supports `nft.fact_nft_transfers` with minting context
- Enables analysis in `stats.ez_core_metrics_hourly` for NFT metrics
- Powers cross-collection analysis and ecosystem mapping

## Commonly-used Fields
- `tx_hash`: Essential for linking to transaction details and verification
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `token_id`: Critical for unique NFT identification and tracking
- `owner_id`: Important for creator analysis and ownership tracking
- `method_name`: Essential for minting method classification and analysis
- `deposit` and `implied_price`: Important for minting cost analysis
- `tx_receiver`: Critical for collection identification and contract analysis

{% enddocs %} 