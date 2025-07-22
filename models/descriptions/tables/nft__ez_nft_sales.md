{% docs nft__ez_nft_sales %}

## Description
This table provides a comprehensive view of all NFT sales on the NEAR Protocol blockchain, combining minting and transfer data with marketplace information, pricing details, and fee structures. The table includes both raw and USD prices, platform fees, royalties, and affiliate information, making it the primary table for NFT marketplace analytics and sales analysis. This easy view eliminates the need for complex joins and calculations, providing ready-to-use data for NFT analytics, reporting, and business intelligence workflows.

## Key Use Cases
- NFT marketplace volume analysis and sales performance tracking
- NFT pricing analysis and market trend identification
- Platform fee analysis and marketplace economics assessment
- Royalty tracking and creator revenue analysis
- Affiliate program analysis and user acquisition tracking
- Cross-platform NFT sales comparison and performance benchmarking
- NFT collection performance analysis and market health monitoring

## Important Relationships
- Combines data from `nft.fact_nft_mints` and `nft.fact_nft_transfers` for comprehensive sales analysis
- Provides sales data for marketplace analytics and platform comparison
- Supports `nft.dim_nft_contract_metadata` with collection context
- Enables analysis in `stats.ez_core_metrics_hourly` for NFT metrics
- Provides foundation for all NFT marketplace analytics and reporting
- Powers cross-collection analysis and ecosystem mapping

## Commonly-used Fields
- `price` and `price_usd`: Essential for sales value analysis and market trends
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `seller_address` and `buyer_address`: Critical for user behavior analysis and flow tracking
- `platform_name`: Important for marketplace comparison and platform analysis
- `token_id` and `nft_address`: Essential for NFT identification and collection analysis
- `platform_fee` and `royalties`: Important for marketplace economics analysis
- `affiliate_id`: Useful for affiliate program analysis and user acquisition tracking

{% enddocs %} 