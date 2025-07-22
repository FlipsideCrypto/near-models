{% docs nft__dim_nft_contract_metadata %}

## Description
This table contains comprehensive metadata for NFT contracts on the NEAR Protocol blockchain, including contract names, symbols, base URIs, and token counts. The data covers NFT collections and individual NFT contracts, providing essential context for NFT identification, collection analysis, and metadata enrichment. This dimension table supports all NFT-related analytics by providing standardized contract information and enabling proper NFT categorization and analysis.

## Key Use Cases
- NFT collection identification and categorization
- NFT contract analysis and collection metadata enrichment
- NFT marketplace integration and collection discovery
- NFT collection performance tracking and analysis
- Cross-collection comparison and benchmarking
- NFT ecosystem mapping and collection discovery
- NFT metadata standardization and quality assessment

## Important Relationships
- Enriches `nft.fact_nft_mints` with collection metadata and contract information
- Supports `nft.fact_nft_transfers` with contract context and collection details
- Provides metadata for `nft.ez_nft_sales` with enhanced collection information
- Enables analysis in `stats.ez_core_metrics_hourly` for NFT metrics
- Supports cross-collection analysis and ecosystem mapping

## Commonly-used Fields
- `contract_address`: Essential for joining with NFT transaction and transfer data
- `name` and `symbol`: Critical for human-readable collection identification
- `base_uri`: Important for NFT metadata access and collection exploration
- `tokens`: Useful for collection size analysis and token count tracking
- `icon`: Important for collection branding and visual identification
- `inserted_timestamp`: Useful for collection discovery timeline analysis

{% enddocs %} 