# Near DBT Project

Curated SQL Views and Metrics for the Near Blockchain.

What's Near? Learn more [here](https://near.org/)

## Setup
### Prerequisites




1. Complete the steps in the [Data Curator Onboarding Guide](https://docs.metricsdao.xyz/data-curation/data-curator-onboarding).
    * Note that the Data Curator Onboarding Guide assumes that you will ask to be added as a contributor to a MetricsDAO project. Ex: https://github.com/MetricsDAO/near_dbt. 
    * However, if you have not yet been added as a contributor, or you'd like to take an even lower-risk approach, you can always follow the [Fork and Pull Workflow](https://reflectoring.io/github-fork-and-pull/) by forking a copy of the project to which you'd like to contribute to a local copy of the project in your github account. Just make sure to: 
        - Fork the MetricsDAO repository.
        - Git clone from your forked repository. Ex: `git clone https://github.com/YourAccount/near_dbt`.
        - Create a branch for the changes you'd like to make. Ex: `git branch readme-update`.
        - Switch to the branch. Ex: `git checkout readme-update`. 
        - Make your changes on the branch and follow the rest of the steps in the [Fork and Pull Workflow](https://reflectoring.io/github-fork-and-pull/) to notify the MetricsDAO repository owners to review your changes. 
2. Download [Docker for Desktop](https://www.docker.com/products/docker-desktop). 
    * (Optional) You can run the Docker tutorial. 
3. Install [VSCode](https://code.visualstudio.com/).

### Prerequisites: Additional Windows Subsystem for Linux (WSL) Setup

4. For Windows users, you'll need to install WSL and connect VSCode to WSL by   
	* Right clicking VSCode and running VSCode as admin.
    * Installing [WSL](https://docs.microsoft.com/en-us/windows/wsl/install) by typing `wsl --install` in VScode's terminal. 
	* Following the rest of the [VSCode WSL instruction](https://code.visualstudio.com/docs/remote/wsl) to create a new WSL user. 
	* Installing the Remote Development extension (ms-vscode-remote.vscode-remote-extensionpack) in VSCode. 
    * Finally, restarting VSCode in a directory in which you'd like to work. For example, 
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

## Background on Data

`CHAINWALKERS.PROD.NEAR_BLOCKS` - Near blocks
`CHAINWALKERS.PROD.NEAR_TXS` - Near txs

Blocks and transactions are fed into the above two Near tables utilizing the Chainwalkers Framework. Details on the data:

1. This is near-real time. Blocks land in this table within 3-5 minutes of being minted.
2. The table is a read-only data share in the Metrics DAO Snowflake account under the database `FLIPSIDE`.
3. The table is append-only, meaning that duplicates can exist if blocks are re-processed. The injested_at timestamp should be used to retrieve only the most recent block. Macros exist `macros/dedupe_utils.sql` to handle this. See `models/core/blocks.sql` or `/models/core/txs.sql` for an example.
4. Tx logs are decoded where an ABI exists.

### Table Structures:

`CHAINWALKERS.PROD.NEAR_BLOCKS` - Near Blocks

| Column          | Type         | Description                                                      |
| --------------- | ------------ | ---------------------------------------------------------------- |
| record_id       | VARCHAR      | A unique id for the record generated by Chainwalkers             |
| offset_id       | NUMBER(38,0) | Synonmous with block_id for Near                                 |
| block_id        | NUMBER(38,0) | The height of the chain this block corresponds with              |
| block_timestamp | TIMESTAMP    | The time the block was minted                                    |
| network         | VARCHAR      | The blockchain network (i.e. mainnet, testnet, etc.)             |
| chain_id        | VARCHAR      | Synonmous with blockchain name for Near                          |
| tx_count        | NUMBER(38,0) | The number of transactions in the block                          |
| header          | json variant | A json queryable column containing the blocks header information |
| ingested_at     | TIMESTAMP    | The time this data was ingested into the table by Snowflake      |

`CHAINWALKERS.PROD.NEAR_TXS` - Near Transactions

| Column          | Type         | Description                                                            |
| --------------- | ------------ | ---------------------------------------------------------------------- |
| record_id       | VARCHAR      | A unique id for the record generated by Chainwalkers                   |
| tx_id           | VARCHAR      | A unique on chain identifier for the transaction                       |
| tx_block_index  | NUMBER(38,0) | The index of the transaction within the block. Starts at 0.            |
| offset_id       | NUMBER(38,0) | Synonmous with block_id for Near                                       |
| block_id        | NUMBER(38,0) | The height of the chain this block corresponds with                    |
| block_timestamp | TIMESTAMP    | The time the block was minted                                          |
| network         | VARCHAR      | The blockchain network (i.e. mainnet, testnet, etc.)                   |
| chain_id        | VARCHAR      | Synonmous with blockchain name for Near                                |
| tx_count        | NUMBER(38,0) | The number of transactions in the block                                |
| header          | json variant | A json queryable column containing the blocks header information       |
| tx              | array        | An array of json queryable objects containing each tx and decoded logs |
| ingested_at     | TIMESTAMP    | The time this data was ingested into the table by Snowflake            |

## Target Database, Schemas and Tables

Data in this DBT project is written to the `NEAR` database in MetricsDAO.

This database has 2 schemas, one for `DEV` and one for `PROD`. As a contributer you have full permission to write to the `DEV` schema. However the `PROD` schema can only be written to by Metric DAO's DBT Cloud account. The DBT Cloud account controls running / scheduling models against the `PROD` schema.

## Branching / PRs

When conducting work please branch off of main with a description branch name and generate a pull request. At least one other individual must review the PR before it can be merged into main. Once merged into main DBT Cloud will run the new models and output the results into the `PROD` schema.

When creating a PR please include the following details in the PR description:

1. List of Tables Created or Modified
2. Description of changes.
3. Implication of changes (if any).

## More DBT Resources:

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
