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

## More DBT Resources:

* Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
* Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
* Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices