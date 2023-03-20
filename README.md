# Near DBT Project

Curated SQL Views and Metrics for the Near Blockchain.

What's Near? Learn more [here](https://near.org/)

## Setup

### Prerequisites

1. Complete the steps in the [Data Curator Onboarding Guide](https://docs.metricsdao.xyz/data-curation/data-curator-onboarding).
    - Note that the Data Curator Onboarding Guide assumes that you will ask to be added as a contributor to a MetricsDAO project. Ex: https://github.com/MetricsDAO/near_dbt.
    - However, if you have not yet been added as a contributor, or you'd like to take an even lower-risk approach, you can always follow the [Fork and Pull Workflow](https://reflectoring.io/github-fork-and-pull/) by forking a copy of the project to which you'd like to contribute to a local copy of the project in your github account. Just make sure to:
        - Fork the MetricsDAO repository.
        - Git clone from your forked repository. Ex: `git clone https://github.com/YourAccount/near_dbt`.
        - Create a branch for the changes you'd like to make. Ex: `git branch readme-update`.
        - Switch to the branch. Ex: `git checkout readme-update`.
        - Make your changes on the branch and follow the rest of the steps in the [Fork and Pull Workflow](https://reflectoring.io/github-fork-and-pull/) to notify the MetricsDAO repository owners to review your changes.
2. Download [Docker for Desktop](https://www.docker.com/products/docker-desktop).
    - (Optional) You can run the Docker tutorial.
3. Install [VSCode](https://code.visualstudio.com/).

### Prerequisites: Additional Windows Subsystem for Linux (WSL) Setup

4. For Windows users, you'll need to install WSL and connect VSCode to WSL by
     + Right clicking VSCode and running VSCode as admin.
       - Installing [WSL](https://docs.microsoft.com/en-us/windows/wsl/install) by typing `wsl --install` in VScode's terminal.
     + Following the rest of the [VSCode WSL instruction](https://code.visualstudio.com/docs/remote/wsl) to create a new WSL user.
     + Installing the Remote Development extension (ms-vscode-remote.vscode-remote-extensionpack) in VSCode.
       - Finally, restarting VSCode in a directory in which you'd like to work. For example,
           - `cd ~/metricsDAO/data_curation/near_dbt`

           - `code .`

### Create the Environment Variables

1. Create a `.env` file with the following contents (note `.env` will not be committed to source) in the near_dbt directory (ex: near_dbt/.env):

```
    SF_ACCOUNT=zsniary-metricsdao
    SF_USERNAME=<your_metrics_dao_snowflake_username>
    SF_PASSWORD=<your_metrics_dao_snowflake_password>
    SF_REGION=us-east-1
    SF_DATABASE=NEAR_DEV
    SF_WAREHOUSE=DEFAULT
    SF_ROLE=PUBLIC
    SF_SCHEMA=SILVER
```

**Replace** the SF_USERNAME and SF_PASSWORD with the temporary Snowflake user name and password you received in the Snowflake step of the [Data Curator Onboarding Guide](https://docs.metricsdao.xyz/data-curation/data-curator-onboarding).

2. New to DBT? It's pretty dope. Read up on it [here](https://www.getdbt.com/docs/)

## Getting Started Commands

Run the following commands from inside the Near directory (**you must have completed the Setup steps above^^**)

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

### DBT Environment

1. In VSCode's terminal, type `cd near_dbt`.
2. Then run `make dbt-console`. This will mount your local near directory into a dbt console where dbt is installed.
    - You can verify that the above command ran successfully by looking at the terminal prompt. It should have changed from your Linux bash prompt to something like root@3527b594aaf0:/near#. Alternatively, you can see in the Docker Desktop app that an instance of near_dbt is now running.

### DBT Project Docs

1. In VSCode, open another terminal.
2. In this new terminal, run `make dbt-docs`. This will compile your dbt documentation and launch a web-server at http://localhost:8080

Documentation is automatically generated and hosted using Netlify.
[![Netlify Status](https://api.netlify.com/api/v1/badges/12fc0079-7428-4771-9923-38ee6599db0f/deploy-status)](https://app.netlify.com/sites/mdao-near/deploys)

## Project Overview

`/models` - this directory contains SQL files as Jinja templates. DBT will compile these templates and wrap them into create table statements. This means all you have to do is define SQL select statements, while DBT handles the rest. The snowflake table name will match the name of the sql model file.

`/macros` - these are helper functions defined as Jinja that can be injected into your SQL models.

`/tests` - custom SQL tests that can be attached to tables.

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

## More DBT Resources:

* Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
* Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
* Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

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

#### Model Tags

To help with targeted refreshes, a number of tags have been applied to the models. These are defined below:

| Tag | Description |
| --- | --- |
| load | Runs models that load data into Snowflake from S3. The 2 `load_X` models are staging tables for data, which is then parsed and transformed up until the final txs/receipts models. |
| receipt_map | Runs the receipt-mapping models that must use a partition. This set of models cannot simply run with incremental logic due to the recursive tree used to map receipt IDs to Tx Hashes. |
| actions | Just the 3 action events models, an important set of intermediary models before curated activity. Note: These are also tagged with `s3_curated`. |
| curated | Models that are used to generate the curated tables |

Note: certain views, like `core__fact_blocks` are not tagged, so running all views in with `-s models/core/` is recommended after changes are made.

### Incremental Load Strategy

 - TODO - comment this section


Include the following conditional, as targeted runs of block partitions may be required:
```
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
```
