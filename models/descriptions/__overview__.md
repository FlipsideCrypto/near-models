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

<llm>
<blockchain>NEAR Protocol</blockchain>
<aliases>NEAR, NEAR Protocol</aliases>
<ecosystem>Layer 1, Sharded Blockchain</ecosystem>
<description>NEAR Protocol is a layer-1 blockchain designed for usability and scalability, featuring a unique sharding architecture called Nightshade that enables high throughput and low latency. Built with a focus on developer experience, NEAR uses a proof-of-stake consensus mechanism and supports both Rust and AssemblyScript for smart contract development. The protocol features human-readable account names, low transaction fees, and fast finality, making it ideal for decentralized applications, DeFi protocols, and NFT marketplaces. NEAR's sharding technology allows the network to scale horizontally by processing transactions across multiple shards simultaneously, while maintaining security and decentralization.</description>
<external_resources>
    <block_scanner>https://explorer.near.org/</block_scanner>
    <developer_documentation>https://docs.near.org/</developer_documentation>
</external_resources>
</llm>

{% enddocs %} 