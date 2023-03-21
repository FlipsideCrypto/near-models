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

If data needs to be re-run for some reason, partitions of data can be re-reun through the models by utilizing the column `_partition_by_block_number` and the dbt profile name.

Any data refresh will need to be done in a batch due to the nature of the receipt x tx hash mapping. The view `silver__receipt_tx_hash_mapping` is a recursive AncestryTree that follows linked receipt outcome ids to map all receipts generated in a transaction back to the primary hash. Receipts can be generated many blocks after the transaction occurs, so a generous buffer is required to ensure all receipts are captured.

Models in the `streamline` folder can be run with standard incremental logic up until the 2 final receipt and transaction tables (tagged as such, see below). The next step, mapping receipts to tx hash over a range, can be run with the following command:

```
dbt run -s tag:receipt_map tag:curated --vars '{"range_start": X, "range_end": Y, "front_buffer": 1, "end_buffer": 1}' -t manual_fix_dev
```

The target name will determine how the model operates, calling a macro `partition_load_manual()` which takes the variables input in the command to set the range.

`front_buffer` and `end_buffer` are set to 1 by default and indicate how many partitions (10k blocks) should be added on to the front or end of the range.
 - Flatten receipts is set to look back 1 partition to grab receipts that may have occurred prior to the start of the range.
 - Receipt tx hash mapping is set to look back 1 partition to grab receipts that may have occurred prior to the start of the range.
 - Receipts final does not buffer the range to only map receipts that occurred within the range to a tx hash (but the lookback is necessary in case the tree is truncated by the partition break).
 - Transactions final does not add a buffer when grabbing transactions from the int txs model, but does add an end buffer when selecting from receipts final to include mapped receipts that may have occurred after the end of the range.

 A range is necessary for the mapping view as it consumes a significant amount of memory and will otherwise run out.

 Actions and curated models include the conditional based on target name so the tags `s3_actions` and `s3_curated` can be included to re-run the fixed data in downstream silver models.
  - if missing data is loaded in new, this is not necessary as `_load_timestamp` will be set to when the data hits snowflake and will flow through the standard incremental logic in the curated models.

#### dbt Model Tags

To help with targeted refreshes, a number of tags have been applied to the models. These are defined below:

| Tag | Description |
| --- | --- |
| load | Runs models that load data into Snowflake from S3. The 2 `load_X` models are staging tables for data, which is then parsed and transformed up until the final txs/receipts models. |
| receipt_map | Runs the receipt-mapping models that must use a partition. This set of models cannot simply run with incremental logic due to the recursive tree used to map receipt IDs to Tx Hashes. |
| actions | Just the 3 action events models, an important set of intermediary models before curated activity. Note: These are also tagged with `s3_curated`. |
| curated | Models that are used to generate the curated tables |
| core | All public views are tagged with core, regardless of schema. At this time, that includes `core` and `social`. |

Note: there are other tags that are currently not used. All legacy models are tagged with something that includes `rpc`, but all are presently disabled to avoid an accidental run.

### Incremental Load Strategy
Because data must be run in batches to properly map receipts to a transaction hash, a conditional is added to curated models using jinja. This should be present on everything after the mapping process.
Most data will have no issue running with a standard incremental load. This filter is required for the above commands in the **Manual Batch Refresh** section.

Include the following conditional, as targeted runs of block partitions may be required:
```
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
```
## More DBT Resources:

* Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
* Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
* Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices