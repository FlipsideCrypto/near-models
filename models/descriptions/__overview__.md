{% docs __overview__ %}

# NEAR Protocol Blockchain Analytics

This dbt project provides comprehensive analytics and data models for the NEAR Protocol blockchain. NEAR is a layer-1 blockchain designed for usability and scalability, featuring a unique sharding architecture and developer-friendly environment. The project transforms raw blockchain data into structured, queryable tables that support analytics, reporting, and business intelligence workflows.

## **Quick Links to Table Documentation**

**Click on the links below to jump to the documentation for each schema.**

### [Core Tables](`NEAR`.`CORE`)

**Dimension Tables:**
- [core__dim_address_labels](#!/model/model.near_models.core__dim_address_labels)
- [core__dim_ft_contract_metadata](#!/model/model.near_models.core__dim_ft_contract_metadata)

**Fact Tables:**
- [core__fact_blocks](#!/model/model.near_models.core__fact_blocks)
- [core__fact_developer_activity](#!/model/model.near_models.core__fact_developer_activity)
- [core__fact_logs](#!/model/model.near_models.core__fact_logs)
- [core__fact_receipts](#!/model/model.near_models.core__fact_receipts)
- [core__fact_token_transfers](#!/model/model.near_models.core__fact_token_transfers)
- [core__fact_transactions](#!/model/model.near_models.core__fact_transactions)

**Easy Views:**
- [core__ez_actions](#!/model/model.near_models.core__ez_actions)
- [core__ez_native_daily_balances](#!/model/model.near_models.core__ez_native_daily_balances)
- [core__ez_token_transfers](#!/model/model.near_models.core__ez_token_transfers)

### [DeFi Tables](`NEAR`.`DEFI`)

**Fact Tables:**
- [defi__fact_bridge_activity](#!/model/model.near_models.defi__fact_bridge_activity)
- [defi__fact_dex_swaps](#!/model/model.near_models.defi__fact_dex_swaps)
- [defi__fact_intents](#!/model/model.near_models.defi__fact_intents)
- [defi__fact_lending](#!/model/model.near_models.defi__fact_lending)

**Easy Views:**
- [defi__ez_bridge_activity](#!/model/model.near_models.defi__ez_bridge_activity)
- [defi__ez_dex_swaps](#!/model/model.near_models.defi__ez_dex_swaps)
- [defi__ez_intents](#!/model/model.near_models.defi__ez_intents)
- [defi__ez_lending](#!/model/model.near_models.defi__ez_lending)

### [NFT Tables](`NEAR`.`NFT`)

**Dimension Tables:**
- [nft__dim_nft_contract_metadata](#!/model/model.near_models.nft__dim_nft_contract_metadata)

**Fact Tables:**
- [nft__fact_nft_mints](#!/model/model.near_models.nft__fact_nft_mints)
- [nft__fact_nft_transfers](#!/model/model.near_models.nft__fact_nft_transfers)

**Easy Views:**
- [nft__ez_nft_sales](#!/model/model.near_models.nft__ez_nft_sales)

### [Price Tables](`NEAR`.`PRICE`)

**Dimension Tables:**
- [price__dim_asset_metadata](#!/model/model.near_models.price__dim_asset_metadata)

**Fact Tables:**
- [price__fact_prices_ohlc_hourly](#!/model/model.near_models.price__fact_prices_ohlc_hourly)

**Easy Views:**
- [price__ez_asset_metadata](#!/model/model.near_models.price__ez_asset_metadata)
- [price__ez_prices_hourly](#!/model/model.near_models.price__ez_prices_hourly)

### [Governance Tables](`NEAR`.`GOV`)

**Dimension Tables:**
- [gov__dim_staking_pools](#!/model/model.near_models.gov__dim_staking_pools)

**Fact Tables:**
- [gov__fact_lockup_actions](#!/model/model.near_models.gov__fact_lockup_actions)
- [gov__fact_staking_actions](#!/model/model.near_models.gov__fact_staking_actions)
- [gov__fact_staking_pool_balances](#!/model/model.near_models.gov__fact_staking_pool_balances)
- [gov__fact_staking_pool_daily_balances](#!/model/model.near_models.gov__fact_staking_pool_daily_balances)

### [Stats Tables](`NEAR`.`STATS`)

**Easy Views:**
- [stats__ez_core_metrics_hourly](#!/model/model.near_models.stats__ez_core_metrics_hourly)

### [Atlas Tables](`NEAR`.`ATLAS`)

**Easy Views:**
- [atlas__ez_supply](#!/model/model.near_models.atlas__ez_supply)

## Data Model Overview

This project follows a dimensional modeling approach with the following layers:

- **Bronze Layer**: Raw data ingestion from blockchain sources
- **Silver Layer**: Cleaned and standardized data with business logic applied
- **Gold Layer**: Curated dimensional models optimized for analytics

The gold layer is organized into domain-specific schemas:
- **Core**: Fundamental blockchain data (blocks, transactions, transfers)
- **DeFi**: Decentralized finance activities (swaps, lending, bridges)
- **NFT**: Non-fungible token operations
- **Price**: Asset pricing and market data
- **Governance**: Staking and protocol governance
- **Stats**: Aggregated metrics and KPIs
- **Atlas**: Supply and economic indicators

## Using dbt docs

To explore the complete data lineage and documentation:

1. Run `dbt docs generate` to create the documentation
2. Run `dbt docs serve` to start the documentation server
3. Navigate to the generated documentation in your browser

The documentation includes:
- Table and column descriptions
- Data lineage graphs
- Test results and data quality metrics
- Model dependencies and relationships

<llm>
<blockchain>NEAR Protocol</blockchain>
<aliases>NEAR, NEAR Protocol</aliases>
<ecosystem>Layer 1, Sharded Blockchain</ecosystem>
<description>NEAR Protocol is a layer-1 blockchain designed for usability and scalability, featuring a unique sharding architecture called Nightshade that enables high throughput and low latency. Built with a focus on developer experience, NEAR uses a proof-of-stake consensus mechanism and supports both Rust and AssemblyScript for smart contract development. The protocol features human-readable account names, low transaction fees, and fast finality, making it ideal for decentralized applications, DeFi protocols, and NFT marketplaces. NEAR's sharding technology allows the network to scale horizontally by processing transactions across multiple shards simultaneously, while maintaining security and decentralization.</description>
<external_resources>
    <block_scanner>https://explorer.near.org/</block_scanner>
    <developer_documentation>https://docs.near.org/</developer_documentation>
</external_resources>
<expert>
  <constraints>
    <table_availability>
      Ensure that your queries use only available tables for NEAR Protocol. The available schemas are: NEAR.CORE, NEAR.DEFI, NEAR.NFT, NEAR.PRICE, NEAR.GOV, NEAR.STATS, and NEAR.ATLAS. Each schema contains specific domain data optimized for particular use cases.
    </table_availability>
    
    <schema_structure>
      Understand that dimensions and facts combine to make ez_ tables. Dimension tables provide reference data (addresses, contract metadata, staking pools), fact tables contain transactional data (blocks, transactions, transfers, swaps), and easy views (ez_*) combine these for simplified analytics. The naming convention follows snake_case with schema prefixes separated by double underscores.
    </schema_structure>
  </constraints>

  <optimization>
    <performance_filters>
      Use filters like block_timestamp over the last N days to improve query speed. For time-series analysis, leverage the incremental nature of fact tables and use appropriate date ranges. Consider using the stats__ez_core_metrics_hourly table for pre-aggregated metrics when available.
    </performance_filters>
    
    <query_structure>
      Use CTEs, not subqueries, as readability is important. Leverage window functions for time-series analysis and aggregations. When joining across schemas, ensure proper indexing on join keys like transaction_hash, block_hash, and account_id.
    </query_structure>
    
    <implementation_guidance>
      Be smart with aggregations, window functions, and complex joins. For large datasets, consider using incremental processing patterns and appropriate clustering keys. Monitor query performance and optimize based on execution plans.
    </implementation_guidance>
  </optimization>

  <domain_mapping>
    <token_operations>
      For token transfers, use ez_token_transfers table in the core schema. This table combines transfer events with metadata and provides a comprehensive view of all token movements across the NEAR ecosystem, including both fungible and non-fungible tokens.
    </token_operations>
    
    <defi_analysis>
      For DeFi analysis, use ez_bridge_activity, ez_dex_swaps, ez_lending, and ez_intents tables in the defi schema. These tables provide aggregated views of decentralized finance activities including cross-chain bridges, decentralized exchanges, lending protocols, and intent-based trading.
    </defi_analysis>
    
    <nft_analysis>
      For NFT queries, utilize ez_nft_sales table in the nft schema. This table combines NFT mint and transfer events to provide a comprehensive view of NFT marketplace activity, including sales, transfers, and metadata.
    </nft_analysis>
    
    <specialized_features>
      The intents data in the defi schema is complex, so ensure you ask clarifying questions about specific intent types and execution patterns. NEAR's sharding architecture may affect transaction ordering and cross-shard communication patterns.
    </specialized_features>
  </domain_mapping>

  <interaction_modes>
    <direct_user>
      Ask clarifying questions when dealing with complex data structures, especially for cross-shard transactions, intent-based trading, and complex DeFi interactions. Provide context about NEAR-specific concepts like account-based architecture and sharding.
    </direct_user>
    
    <agent_invocation>
      When invoked by another AI agent, respond with relevant query text and explain the data model structure. Provide guidance on appropriate table selection and optimization strategies for the specific use case.
    </agent_invocation>
  </interaction_modes>

  <engagement>
    <exploration_tone>
      Have fun exploring the NEAR Protocol ecosystem through data! The platform's unique features like human-readable accounts, sharding architecture, and developer-friendly environment provide rich opportunities for blockchain analytics and insights.
    </exploration_tone>
  </engagement>
</expert>
</llm>

{% enddocs %} 