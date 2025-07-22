{% docs core__dim_address_labels %}

## Description
This table contains comprehensive address labeling and categorization data for the NEAR Protocol blockchain, providing human-readable names, project associations, and classification information for accounts and contracts. The data includes both manual and automated labeling from multiple sources, enabling enhanced analytics by providing context about wallet ownership, contract purposes, and project affiliations. This dimension table supports all other gold models by providing enriched address information for improved data interpretation and user experience.

## Key Use Cases
- Enhanced transaction analysis with human-readable address names
- Project-specific activity tracking and protocol analysis
- Whale and institutional wallet identification and monitoring
- Contract categorization and smart contract ecosystem analysis
- Compliance and regulatory reporting with entity identification
- User experience improvement in analytics dashboards and reports
- Cross-protocol address correlation and analysis

## Important Relationships
- Enriches all fact tables by providing address context and labels
- Supports `core.ez_token_transfers` with sender and receiver labeling
- Enhances `defi.ez_dex_swaps` with protocol and user identification
- Improves `defi.ez_bridge_activity` with bridge protocol labeling
- Enables better analysis in `nft.ez_nft_sales` with marketplace identification
- Powers `stats.ez_core_metrics_hourly` with labeled activity metrics

## Commonly-used Fields
- `address`: Essential for joining with transaction and transfer data
- `address_name`: Critical for human-readable analytics and reporting
- `project_name`: Important for protocol-specific analysis and categorization
- `label_type` and `label_subtype`: Essential for address categorization and filtering
- `l1_label` and `l2_label`: Important for hierarchical classification analysis
- `creator`: Useful for understanding label source and reliability

{% enddocs %} 