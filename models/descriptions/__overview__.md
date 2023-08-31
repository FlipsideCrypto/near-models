{% docs __overview__ %}

# Welcome to the Flipside Crypto NEAR Models Documentation

## **What does this documentation cover?**

The documentation included here details the design of the NEAR
tables and views available via [Flipside Crypto.](https://flipsidecrypto.xyz/) For more information on how these models are built, please see [the github repository.](https://github.com/flipsideCrypto/near-models/)

## **How do I use these docs?**

The easiest way to navigate this documentation is to use the Quick Links below. These links will take you to the documentation for each table, which contains a description, a list of the columns, and other helpful information.

If you are experienced with dbt docs, feel free to use the sidebar to navigate the documentation, as well as explore the relationships between tables and the logic building them.

There is more information on how to use dbt docs in the last section of this document.

## **Quick Links to Table Documentation**

**Click on the links below to jump to the documentation for each schema.**

### Core Tables (`NEAR`.`CORE`.`<table_name>`)

**Dimension Tables:**

- [dim_address_labels](#!/model/model.near.core__dim_address_labels)
- [dim_token_labels](#!/model/model.near.core__dim_token_labels)

**Fact Tables:**

- [fact_actions_events_function_call](#!/model/model.near.core__fact_actions_events_function_call)
- [fact_actions_events](#!/model/model.near.core__fact_actions_events)
- [fact_blocks](#!/model/model.near.core__fact_blocks)
- [fact_logs](#!/model/model.near.core__fact_logs)
- [fact_receipts](#!/model/model.near.core__fact_receipts)
- [fact_token_metadata](#!/model/model.near.core__fact_token_metadata)
- [fact_transactions](#!/model/model.near.core__fact_transactions)
- [fact_transfers](#!/model/model.near.core__fact_transfers)

### DeFi Tables (`NEAR`.`DEFI`.`<table_name>`)

- [ez_swaps](#!/model/model.near.defi__ez_dex_swaps)

### Governance Tables (`NEAR`.`GOV`.`<table_name>`)

- [dim_staking_pools](#!/model/model.near.gov__dim_staking_pools)
- [fact_lockup_actions](#!/model/model.near.gov__fact_lockup_actions)
- [fact_staking_actions](#!/model/model.near.gov__fact_staking_actions)
- [fact_staking_pool_balances](#!/model/model.near.gov__fact_staking_pool_balances)
- [fact_staking_pool_daily_balances](#!/model/model.near.gov__fact_staking_pool_daily_balances)

### NFT Tables (`NEAR`.`NFT`.`<table_name>`)

- [fact_nft_mints](#!/model/model.near.nft__fact_nft_mints)

### Price Tables (`NEAR`.`PRICE`.`<table_name>`)

- [fact_prices](#!/model/model.near.price__fact_prices)

### Social Tables (`NEAR`.`SOCIAL`.`<table_name>`)

- [fact_addkey_events](#!/model/model.near.social__fact_addkey_events)
- [fact_decoded_actions](#!/model/model.near.social__fact_decoded_actions)
- [fact_profile_changes](#!/model/model.near.social__fact_profile_changes)
- [fact_posts](#!/model/model.near.social__fact_posts)
- [fact_widget_deployments](#!/model/model.near.social__fact_widget_deployments)

### Beta Tables (`NEAR`.`BETA`.`<table_name>`)

- [github_activity](https://github.com/forgxyz/developer_report_near)

### Custom Functions

- [udtf_call_contract_function](#!/macro/macro.near.create_UDTF_CALL_CONTRACT_FUNCTION_BY_HEIGHT)

Call a contract method via the [public NEAR RPC endpoint](https://docs.near.org/api/rpc/setup), modeled after the official documentation, [here](https://docs.near.org/api/rpc/contracts#call-a-contract-function).

This function accepts 3 or 4 parameters:

- `account_id` STR (required)
- This is the deployed contract_address you want to call.

- `method_name` STR (required)
- This is the method on the contract to call.

- `args` OBJ (required)
- Any requred or optional input parameters that the contract method accepts.
- For best results, this should be formed by using the Snowflake function [`OBJECT_CONSTRUCT()`](https://docs.snowflake.com/en/sql-reference/functions/object_construct)

- `block_id` INT (optional)
- Pass a block height (note - hash not accepted) to call the method at a certain block in time.
- If nothing is passed, the default behavior is `final` per the explanation [here](https://docs.near.org/api/rpc/setup#using-finality-param).
- Note - when passing in a block id parameter, the archive node is called which may be considerably slower than the primary access node.

**Important Note** - this is the public access endpoint, use responsibly.

#### Examples

Return 25 accounts that have authorized the contract `social.near`.

```sql
SELECT
    *
FROM
    TABLE(
        near.core.udtf_call_contract_function(
            'social.near',
            'get_accounts',
            OBJECT_CONSTRUCT(
                'from_index',
                0,
                'limit',
                25
            )
        )
    );

```

Get the staked balance of 100 addresses on the pool `staked.poolv1.near` at block `85,000,000`.

```sql
SELECT
    DATA :result :block_height :: NUMBER AS block_height,
    VALUE :account_id :: STRING AS account_id,
    VALUE :can_withdraw :: BOOLEAN AS can_withdraw,
    VALUE :staked_balance :: NUMBER / pow(
        10,
        24
    ) AS staked_balance,
    VALUE :unstaked_balance :: NUMBER / pow(
        10,
        24
    ) AS unstaked_balance
FROM
    TABLE(
        near.core.udtf_call_contract_function(
            'staked.poolv1.near',
            'get_accounts',
            {
                'from_index': 0,
                'limit': 100
            },
            85000000
        )
    ),
    LATERAL FLATTEN(decoded_result)
```

## **Data Model Overview**

The NEAR
models are built a few different ways, but the core fact tables are built using three layers of sql models: **bronze, silver, and gold (or core).**

- Bronze: Data is loaded in from the source as a view
- Silver: All necessary parsing, filtering, de-duping, and other transformations are done here
- Gold (or core): Final views and tables that are available publicly

The dimension tables are sourced from a variety of on-chain and off-chain sources.

Convenience views (denoted ez\_) are a combination of different fact and dimension tables. These views are built to make it easier to query the data.

## **Using dbt docs**

### Navigation

You can use the `Project` and `Database` navigation tabs on the left side of the window to explore the models in the project.

### Database Tab

This view shows relations (tables and views) grouped into database schemas. Note that ephemeral models are _not_ shown in this interface, as they do not exist in the database.

### Graph Exploration

You can click the blue icon on the bottom-right corner of the page to view the lineage graph of your models.

On model pages, you'll see the immediate parents and children of the model you're exploring. By clicking the Expand button at the top-right of this lineage pane, you'll be able to see all of the models that are used to build, or are built from, the model you're exploring.

Once expanded, you'll be able to use the `--models` and `--exclude` model selection syntax to filter the models in the graph. For more information on model selection, check out the [dbt docs](https://docs.getdbt.com/docs/model-selection-syntax).

Note that you can also right-click on models to interactively filter and explore the graph.

### **More information**

- [Flipside](https://flipsidecrypto.xyz/)
- [Velocity](https://app.flipsidecrypto.com/velocity?nav=Discover)
- [Tutorials](https://docs.flipsidecrypto.com/our-data/tutorials)
- [Github](https://github.com/FlipsideCrypto/near-models)
- [What is dbt?](https://docs.getdbt.com/docs/introduction)

{% enddocs %}
