# Near DBT Project

Curated SQL Views and Metrics for the Near Blockchain.

What's Near? Learn more [here](https://near.org/)

## Variables

To control which external table environment a model references, as well as, whether a Stream is invoked at runtime using control variables:
* STREAMLINE_INVOKE_STREAMS
When True, invokes streamline on model run as normal
When False, NO-OP
* STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES
When True, uses DEV schema Streamline. Ethereum_DEV
When False, uses PROD schema Streamline. Ethereum

Default values are False

* Usage:
 `dbt run --var '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True, "STREAMLINE_INVOKE_STREAMS": True}'  -m ...`

To control the creation of UDF or SP macros with dbt run:
* UPDATE_UDFS_AND_SPS
When True, executes all macros included in the on-run-start hooks within dbt_project.yml on model run as normal
When False, none of the on-run-start macros are executed on model run

Default values are False

* Usage:
 `dbt run --var '{"UPDATE_UDFS_AND_SPS": True}'  -m ...`

## Applying Model Tags

### Database / Schema level tags

Database and schema tags are applied via the `add_database_or_schema_tags` macro.  These tags are inherited by their downstream objects.  To add/modify tags call the appropriate tag set function within the macro.

```
{{ set_database_tag_value('SOME_DATABASE_TAG_KEY','SOME_DATABASE_TAG_VALUE') }}
{{ set_schema_tag_value('SOME_SCHEMA_TAG_KEY','SOME_SCHEMA_TAG_VALUE') }}
```

### Model tags

To add/update a model's snowflake tags, add/modify the `meta` model property under `config`.  Only table level tags are supported at this time via DBT.

```
{{ config(
    ...,
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'SOME_PURPOSE'
            }
        }
    },
    ...
) }}
```

By default, model tags are pushed to Snowflake on each DBT run. You can disable this by setting the `UPDATE_SNOWFLAKE_TAGS` project variable to `False` during a run.

```
dbt run --var '{"UPDATE_SNOWFLAKE_TAGS":False}' -s models/core/core__ez_dex_swaps.sql
```

### Querying for existing tags on a model in snowflake

```
select *
from table(near.information_schema.tag_references('near.core.ez_dex_swaps', 'table'));
```

## Branching / PRs

When conducting work please branch off of main with a description branch name and generate a pull request. At least one other individual must review the PR before it can be merged into main. Once merged into main DBT Cloud will run the new models and output the results into the `PROD` schema.

When creating a PR please include the following details in the PR description:

1. List of Tables Created or Modified
2. Description of changes.
3. Implication of changes (if any).

## Fixing Data Issues

### Manual Batch Refresh

If data needs to be re-run for some reason, partitions of data can be re-reun through the models by utilizing the column `_partition_by_block_number` and passing environment variables.

Any data refresh will need to be done in a batch due to the nature of the receipt <> tx hash mapping. The view `silver__receipt_tx_hash_mapping` is a recursive AncestryTree that follows linked receipt outcome ids to map all receipts generated in a transaction back to the primary hash. Receipts can be generated many blocks after the transaction occurs, so a generous buffer is required to ensure all receipts are captured.
  
The fix makes use of [project variables](https://docs.getdbt.com/docs/build/project-variables#defining-variables-on-the-command-line) to pass the following parameters:
 - MANUAL_FIX (required): This will run the models with the specified range, rather than the standard incremental logic. `False` by default.
 - RANGE_START (required): The start of the block partition range (nearest 10,000) to run.
 - RANGE_END (required): The end of the block partition range (nearest 10,000) to run.
 - FRONT_BUFFER (optional): The number of partitions to add to the front of the range. 1 by default, not likely to need changing.
 - END_BUFFER (optional): The number of partitions to add to the end of the range. 1 by default, not likely to need changing.


`FRONT_BUFFER` and `END_BUFFER` are set to 1 by default and indicate how many partitions (10k blocks) should be added on to the front or end of the range.
 - Flatten receipts is set to look back 1 partition to grab receipts that may have occurred prior to the start of the range.
 - Receipt tx hash mapping is set to look back 1 partition to grab receipts that may have occurred prior to the start of the range.
 - Receipts final does not buffer the range to only map receipts that occurred within the range to a tx hash (but the lookback is necessary in case the tree is truncated by the partition break).
 - Transactions final does not add a buffer when grabbing transactions from the int txs model, but does add an end buffer when selecting from receipts final to include mapped receipts that may have occurred after the end of the range.

```
dbt run -s [tags] --vars [variables]
```

#### dbt Model Tags

To help with targeted refreshes, a number of tags have been applied to the models. These are defined below:

| Tag | Description | View Models |
| --- | --- | --- | 
| load | Runs models that load data into Snowflake from S3. The 2 `load_X` models are staging tables for data, which is then parsed and transformed up until the final txs/receipts models. | [link](https://flipsidecrypto.github.io/near-models/#!/overview?g_v=1&g_i=tag:load) |
| load_shards | Just the `load` models that touch shards | [link](https://flipsidecrypto.github.io/near-models/#!/overview?g_v=1&g_i=tag:load_shards) |
| load_blocks | Just the `load` models that touch blocks | [link](https://flipsidecrypto.github.io/near-models/#!/overview?g_v=1&g_i=tag:load_blocks) |
| receipt_map | Runs the receipt-mapping models that must use a partition. This set of models cannot simply run with incremental logic due to the recursive tree used to map receipt IDs to Tx Hashes. | [link](https://flipsidecrypto.github.io/near-models/#!/overview?g_v=1&g_i=tag:receipt_map) |
| actions | Just the 3 action events models, an important set of intermediary models before curated activity. Note: These are also tagged with `s3_curated`. | [link](https://flipsidecrypto.github.io/near-models/#!/overview?g_v=1&g_i=tag:actions) |
| curated | Models that are used to generate the curated tables | [link](https://flipsidecrypto.github.io/near-models/#!/overview?g_v=1&g_i=tag:curated) |
| core | All public views are tagged with core, regardless of schema. At this time, that includes `core` and `social`. | [link](https://flipsidecrypto.github.io/near-models/#!/overview?g_v=1&g_i=tag:core) |

Note: there are other tags that are currently not used. All legacy models are tagged with something that includes `rpc`, but all are presently disabled to avoid an accidental run.  
You can visualize these tags by using the DAG Explorer in the [docs](https://flipsidecrypto.github.io/near-models/#!/overview?g_v=1&g_i=tag:load).  


#### Load Missing Blocks or Shards
[Blocks](models/silver/streamline/silver__load_blocks.sql) and [shards](models/silver/streamline/silver__load_shards.sql) can be re-loaded using the load tag(s).
 - `load` will run both blocks and shards models, landing data in `silver__streamline_blocks`, `silver__streamline_receipts`, `silver__streamline_receipt_chunks`, and `silver__streamline_transactions`.
 - `load_blocks` will run just the blocks models, landing data in `silver__streamline_blocks`.
 - `load_shards` will run just the shards models, landing data in `silver__streamline_receipts`, `silver__streamline_receipt_chunks`, and `silver__streamline_transactions`.

The logic in the load_x models will only check the external table for blocks and shards known to be missing. It will query the sequence gap test table(s), thus the partition range is not required for this step, as it will take that from the test table.

```
dbt run -s tag:load --vars '{"MANUAL_FIX": True}'
```

#### Map Tx Hash <> Receipt Hash
The middle step is to map receipt IDs to transaction hashes. This is done in 3 models, which are tagged with `receipt_map`. 2 of these models are helper views that recursively map out the receipt->parent receipt->...->transaction, thus linking all receipts to a transaction. This step is computationally intensive, and requires a tight partition range. For present blocks with more activity, <250k is recommended. 

The following will read a range of receipts from `silver__streamline_receipts` and link receipts to the corresponding tx hash, saving the result to `silver__streamline_receipts_final`. It will then insert receipt data into the `tx` object in `silver__streamline_transactions_final`.

```
dbt run -s tag:receipt_map --vars '{"MANUAL_FIX": True, "RANGE_START": X, "RANGE_END": Y}'
```

If the range being mapped is the same range as the block/shard re-walk, then the tag can simply be appended to the load job and the receipts will be mapped after ingestion.
```
dbt run -s tag:load tag:receipt_map --vars '{"MANUAL_FIX": True, "RANGE_START": X, "RANGE_END": Y}'
```

The end result of this run will be `streamline__receipts_final` and `streamline__transactions_final` ([link](https://flipsidecrypto.github.io/near-models/#!/overview?g_v=1&g_i=tag:load%20tag:receipt_map)).

#### Update curated models
 Actions and curated models include the conditional based on target name so the tags `actions` and `curated` can be included to re-run the fixed data in downstream silver models. If missing data is loaded in new, this should not be necessary as `_load_timestamp` will be set to when the data hits snowflake and will flow through the standard incremental logic in the curated models. However, the range can be run with the curated tag:

 ```
dbt run -s tag:curated --vars '{"MANUAL_FIX": True, "RANGE_START": X, "RANGE_END": Y}'
```
Or
```
dbt run -s tag:load tag:receipt_map tag:curated --vars '{"MANUAL_FIX": True, "RANGE_START": X, "RANGE_END": Y}'
```

### Incremental Load Strategy
Because data must be run in batches to properly map receipts to a transaction hash, a conditional is added to curated models using jinja. This should be present on everything after the mapping process.
Most data will have no issue running with a standard incremental load. This filter is required for the above commands in the **Manual Batch Refresh** section.

Include the following conditional, as targeted runs of block partitions may be required:
```
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_inserted_timestamp') }}
        {% endif %}
```
## More DBT Resources:

* Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
* Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
* Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices