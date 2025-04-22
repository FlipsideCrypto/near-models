# Migration Changelog: Actions Model Refactor

This changelog tracks all model changes related to the deprecation of silver actions models in favor of core__ez_actions.

## silver__nft_transfers

### Major Changes
- **REQUIRES FULL REFRESH** - Model has been completely refactored to use `silver__logs_s3` directly
- Previous version had data duplication due to incorrect handling of many-to-many relationship between actions and logs
- New version correctly processes NFT transfer logs without duplication
- No manual column changes needed as model will be rebuilt from scratch

### Column Changes

#### Columns Removed
- `action_id` (was used for unique identification, replaced by receipt_id)
- `_inserted_timestamp` (deprecated)

#### Columns Added
- `receipt_id` (from logs table)
- `predecessor_id` (from logs table)
- `_partition_by_block_number` (calculated as `FLOOR(block_id, -3)`)

#### Column Modifications
- Changed `contract_id` to `receiver_id` in initial CTE, still aliased as `contract_address` in final output
- Added type casting for `token_ids` as ARRAY
- Added type casting for event parsing with `TRY_PARSE_JSON(clean_log)`

### Configuration Changes
- Updated incremental predicates to use `dynamic_range_predicate_custom`
- Added `modified_timestamp::DATE` to clustering keys
- Updated search optimization to use raw column names (removed block_timestamp::DATE expression)
- Updated unique key components in surrogate key generation

### Downstream Impact
The gold model `nft__fact_nft_transfers.sql` needs to be updated:
1. Replace `action_id` with `receipt_id` in SELECT statement
2. Remove `_inserted_timestamp` from COALESCE logic in inserted_timestamp/modified_timestamp

---

## silver__burrow_borrows

### Major Changes
- Model has been refactored to use `core__ez_actions` directly
- Maintains same business logic and data structure, allowing for incremental processing

### Architecture Changes
- Simplified to single CTE structure using `core__ez_actions` for function call data
- Improved args parsing with proper JSON handling for borrow actions
- Added proper handling of segmented_data for borrow amount extraction

### Column Changes

#### Columns Removed
- `action_id` (replaced by receipt_id + action_index combination)
- `_inserted_timestamp` (deprecated)
- Removed dependency on deprecated `logs` column from actions table

#### Columns Added
- `receipt_id` (from core__ez_actions)
- `action_index` (from core__ez_actions)
- `predecessor_id` (from receipt_predecessor_id)

#### Column Modifications
- Changed `receiver_id` to come from `receipt_receiver_id`
- Changed `method_name` to parse from `action_data :method_name`
- Changed `args` to parse from `action_data :args`
- Changed `_partition_by_block_number` to be calculated as `FLOOR(block_id, -3)`
- Added COALESCE for amount_raw to handle both amount and max_amount fields
- Changed segmented_data parsing to use proper JSON path for borrow actions

### Configuration Changes
- Updated incremental predicates to use `dynamic_range_predicate_custom`
- Added `modified_timestamp::DATE` to clustering keys
- Added `receipt_id` to search optimization
- Updated unique key to use combination of `receipt_id` and `action_index`

### Query Changes
- Updated WHERE clause to filter on `action_name = 'FunctionCall'` first, then check `action_data :method_name`
- Updated partition_load_manual to include partition key calculation
- Added proper type casting for method_name and args from action_data
- Added explicit NULL check on segmented_data in WHERE clause
- Improved JSON parsing for borrow action data using proper path: `args:msg:Execute:actions[0]:Borrow`

---

## silver__burrow_collaterals

### Major Changes
- Model has been refactored to use both `core__ez_actions` and `silver__logs_s3` directly
- Added proper handling of logs by joining directly to `silver__logs_s3` instead of using deprecated logs column
- Maintains same business logic and data structure, allowing for incremental processing

### Architecture Changes
- Split data sourcing into two CTEs:
  1. `actions` CTE from `core__ez_actions` for function call data
  2. `logs` CTE from `silver__logs_s3` for event logs
- Added smart log targeting using `target_log_index` calculation based on method_name
- Implemented proper join logic between actions and logs using tx_hash, receipt_id, and calculated log_index

### Column Changes

#### Columns Removed
- `action_id` (replaced by receipt_id + action_index combination)
- `_inserted_timestamp` (deprecated)
- Removed dependency on deprecated `logs` column from actions table

#### Columns Added
- `receipt_id` (from core__ez_actions)
- `action_index` (from core__ez_actions)
- `predecessor_id` (from receipt_predecessor_id)
- `signer_id` (from receipt_signer_id)
- `target_log_index` (calculated from method_name)

#### Column Modifications
- Changed `receiver_id` to come from `receipt_receiver_id`
- Changed `method_name` to parse from `action_data :method_name`
- Changed `args` to parse from `action_data :args`
- Changed `_partition_by_block_number` to be calculated as `FLOOR(block_id, -3)`
- Changed log parsing to use clean_log directly from logs table

### Configuration Changes
- Updated incremental predicates to use `dynamic_range_predicate_custom`
- Added `modified_timestamp::DATE` to clustering keys
- Added `receipt_id` to search optimization
- Updated unique key to use combination of `receipt_id` and `action_index`

### Query Changes
- Updated WHERE clause to filter on `action_name = 'FunctionCall'` and specific method_names upfront
- Updated partition_load_manual to include partition key calculation
- Added proper type casting for method_name and args from action_data
- Implemented LEFT JOIN to logs table with proper matching conditions
- Maintained filtering logic for increase_collateral and decrease_collateral actions

---

## silver__burrow_deposits

### Major Changes
- Model has been refactored to use both `core__ez_actions` and `silver__logs_s3` directly
- Added proper handling of logs by joining directly to `silver__logs_s3` instead of using deprecated logs column
- Maintains same business logic and data structure, allowing for incremental processing

### Architecture Changes
- Split data sourcing into two CTEs:
  1. `actions` CTE from `core__ez_actions` for function call data
  2. `logs` CTE from `silver__logs_s3` for event logs
- Simplified log handling by directly targeting log_index = 0 (deposits always use first log)
- Implemented proper join logic between actions and logs using tx_hash and receipt_id

### Column Changes

#### Columns Removed
- `action_id` (replaced by receipt_id + action_index combination)
- `_inserted_timestamp` (deprecated)
- Removed dependency on deprecated `logs` column from actions table

#### Columns Added
- `receipt_id` (from core__ez_actions)
- `action_index` (from core__ez_actions)
- `predecessor_id` (from receipt_predecessor_id)
- `signer_id` (from receipt_signer_id)

#### Column Modifications
- Changed `receiver_id` to come from `receipt_receiver_id`
- Changed `method_name` to parse from `action_data :method_name`
- Changed `args` to parse from `action_data :args`
- Changed `_partition_by_block_number` to be calculated as `FLOOR(block_id, -3)`
- Changed log parsing to use clean_log directly from logs table

### Configuration Changes
- Updated incremental predicates to use `dynamic_range_predicate_custom`
- Added `modified_timestamp::DATE` to clustering keys
- Added `receipt_id` to search optimization
- Updated unique key to use combination of `receipt_id` and `action_index`

### Query Changes
- Updated WHERE clause to filter on `action_name = 'FunctionCall'` and method_name upfront
- Updated partition_load_manual to include partition key calculation
- Added proper type casting for method_name and args from action_data
- Implemented LEFT JOIN to logs table with proper matching conditions
- Simplified log parsing by removing SUBSTRING operation and using TRY_PARSE_JSON directly

---

## silver__burrow_repays

### Major Changes
- Model has been refactored to use both `core__ez_actions` and `silver__logs_s3` directly
- Added proper handling of logs by joining directly to `silver__logs_s3` instead of using deprecated logs column
- Maintains same business logic and data structure, allowing for incremental processing

### Architecture Changes
- Split data sourcing into two CTEs:
  1. `actions` CTE from `core__ez_actions` for function call data
  2. `logs` CTE from `silver__logs_s3` for event logs
- Simplified log handling by directly targeting log_index = 1 (repays always use second log)
- Implemented proper join logic between actions and logs using tx_hash and receipt_id

### Column Changes

#### Columns Removed
- `action_id` (replaced by receipt_id + action_index combination)
- `_inserted_timestamp` (deprecated)
- Removed dependency on deprecated `logs` column from actions table

#### Columns Added
- `receipt_id` (from core__ez_actions)
- `action_index` (from core__ez_actions)
- `predecessor_id` (from receipt_predecessor_id)
- `signer_id` (from receipt_signer_id)

#### Column Modifications
- Changed `receiver_id` to come from `receipt_receiver_id`
- Changed `method_name` to parse from `action_data :method_name`
- Changed `args` to parse from `action_data :args`
- Changed `_partition_by_block_number` to be calculated as `FLOOR(block_id, -3)`
- Changed log parsing to use clean_log directly from logs table

### Configuration Changes
- Updated incremental predicates to use `dynamic_range_predicate_custom`
- Added `modified_timestamp::DATE` to clustering keys
- Added `receipt_id` to search optimization
- Updated unique key to use combination of `receipt_id` and `action_index`

### Query Changes
- Updated WHERE clause to filter on `action_name = 'FunctionCall'` and method_names upfront
- Added method_name IN clause to filter both ft_on_transfer and oracle_on_call
- Updated partition_load_manual to include partition key calculation
- Added proper type casting for method_name and args from action_data
- Implemented LEFT JOIN to logs table with proper matching conditions
- Maintained complex filtering logic for repay actions and args:msg conditions

---

## silver__burrow_withdraws

### Major Changes
- Model has been refactored to use both `core__ez_actions` and `silver__logs_s3` directly
- Added proper handling of logs by joining directly to `silver__logs_s3` instead of using deprecated logs column
- Maintains same business logic and data structure, allowing for incremental processing

### Architecture Changes
- Split data sourcing into two CTEs:
  1. `actions` CTE from `core__ez_actions` for function call data
  2. `logs` CTE from `silver__logs_s3` for event logs
- Simplified log handling by directly targeting log_index = 0 (withdraws always use first log)
- Implemented proper join logic between actions and logs using tx_hash and receipt_id

### Column Changes

#### Columns Removed
- `action_id` (replaced by receipt_id + action_index combination)
- `_inserted_timestamp` (deprecated)
- Removed dependency on deprecated `logs` column from actions table

#### Columns Added
- `receipt_id` (from core__ez_actions)
- `action_index` (from core__ez_actions)
- `predecessor_id` (from receipt_predecessor_id)
- `signer_id` (from receipt_signer_id)

#### Column Modifications
- Changed `receiver_id` to come from `receipt_receiver_id`
- Changed `method_name` to parse from `action_data :method_name`
- Changed `args` to parse from `action_data :args`
- Changed `_partition_by_block_number` to be calculated as `FLOOR(block_id, -3)`
- Changed log parsing to use clean_log directly from logs table

### Configuration Changes
- Updated incremental predicates to use `dynamic_range_predicate_custom`
- Added `modified_timestamp::DATE` to clustering keys
- Added `receipt_id` to search optimization
- Updated unique key to use combination of `receipt_id` and `action_index`

### Query Changes
- Updated WHERE clause to filter on `action_name = 'FunctionCall'` and method_name upfront
- Updated partition_load_manual to include partition key calculation
- Added proper type casting for method_name and args from action_data
- Implemented LEFT JOIN to logs table with proper matching conditions
- Simplified log parsing by removing SUBSTRING operation and using TRY_PARSE_JSON directly

--- 