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

## silver__bridge_rainbow

### Major Changes
- Refactored to use core__ez_actions and silver__logs_s3 instead of silver__actions_events_function_call_s3
- Maintained complex multi-directional bridge logic (Near↔Aurora, Near↔Ethereum)
- Enhanced log parsing with direction-specific strategies:
  * Aurora inbound: Using log_index=0 for source address extraction
  * ETH inbound: Complex log pattern matching for amount and address extraction
  * Near outbound: Direct args parsing from function calls
- Preserved all existing bridge direction handling with improved data sourcing

Architecture Changes:
- Split into specialized CTEs for each bridge direction:
  * outbound_near_to_aurora: ft_transfer_call to Aurora
  * inbound_aurora_to_near: ft_transfer from relay.aurora
  * outbound_near_to_eth: finish_withdraw on factory.bridge.near
  * inbound_eth_to_near: finish_deposit with complex log parsing
- Added dedicated logs CTE with proper log aggregation
- Improved transaction identification using receipt_id matching

Column Changes:
- Added:
  * receipt_id (for more precise transaction tracking)
  * action_index (from core__ez_actions)
- Modified:
  * Improved source/destination address extraction using proper string manipulation
  * Enhanced amount parsing from logs with multiple fallback patterns
  * Simplified bridge_rainbow_id generation using receipt_id
- Removed:
  * Deprecated _inserted_timestamp field

Configuration Changes:
- Updated incremental predicates to use dynamic_range_predicate_custom
- Added modified_timestamp::DATE to clustering keys
- Enhanced search optimization for transaction tracking

Query Changes:
- Improved ETH bridge log parsing with multiple regex patterns for different scenarios
- Enhanced Aurora bridge transaction matching using receipt_id
- Added proper type casting and NULL handling for all extracted fields
- Maintained all existing bridge direction logic while improving data extraction
- Added sophisticated log aggregation for multi-log transactions

---

## silver__bridge_allbridge

### Major Changes
- Model has been refactored to use core__ez_actions directly
- Maintains same business logic and data structure, allowing for incremental processing
- Preserved both bridge directions (outbound NEAR burns and inbound NEAR mints)

### Architecture Changes
- Simplified to use core__ez_actions for function call data
- Improved args parsing with proper JSON handling for both lock and unlock actions
- Maintained metadata join for token decimals handling

### Column Changes

#### Columns Removed
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
- Maintained amount_adj calculation using metadata decimals

### Configuration Changes
- Updated incremental predicates to use `dynamic_range_predicate_custom`
- Added `modified_timestamp::DATE` to clustering keys
- Updated unique key to use combination of `receipt_id` and `action_index`
- Maintained search optimization on tx_hash, destination_address, and source_address

### Query Changes
- Updated WHERE clause to filter on `action_name = 'FunctionCall'` and specific receiver_id upfront
- Updated partition_load_manual to include partition key calculation
- Added proper type casting for method_name and args from action_data
- Maintained separate CTEs for outbound and inbound transfers
- Preserved token metadata join for decimal handling

---

## silver__bridge_multichain

### Major Changes
- Model has been refactored to use core__ez_actions directly
- Maintains same business logic and data structure, allowing for incremental processing
- Preserved both bridge directions (inbound and outbound transfers)

### Architecture Changes
- Simplified to use core__ez_actions for function call data
- Maintained separate CTEs for inbound and outbound transfers
- Preserved memo parsing logic for chain ID and address extraction

### Column Changes

#### Columns Removed
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
- Maintained memo parsing for chain IDs and addresses

### Configuration Changes
- Updated incremental predicates to use `dynamic_range_predicate_custom`
- Added `modified_timestamp::DATE` to clustering keys
- Updated unique key to use combination of `receipt_id` and `action_index`
- Maintained search optimization on tx_hash, destination_address, and source_address

### Query Changes
- Updated WHERE clause to filter on `action_name = 'FunctionCall'` and specific method_name upfront
- Updated partition_load_manual to include partition key calculation
- Added proper type casting for method_name and args from action_data
- Maintained separate CTEs for inbound and outbound transfers with their specific filtering logic:
  * Inbound: signer_id = 'mpc-multichain.near'
  * Outbound: args:receiver_id = 'mpc-multichain.near'
- Preserved memo parsing logic for extracting chain IDs and addresses

---

## silver__bridge_wormhole

### Major Changes
- Model has been refactored to use both `core__ez_actions` and `silver__logs_s3` directly
- Maintains same business logic and data structure, allowing for incremental processing
- Preserved both bridge directions (outbound withdraws and inbound transfers)

### Architecture Changes
- Split data sourcing into two CTEs:
  1. `actions` CTE from `core__ez_actions` for function call data
  2. `logs` CTE from `silver__logs_s3` with optimized filtering to only fetch relevant transaction logs
- Improved log parsing by using clean_log field directly
- Maintained separate CTEs for inbound and outbound transfers
- Enhanced source chain ID extraction from logs for inbound transfers with precise log targeting

### Column Changes

#### Columns Removed
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
- Improved source chain ID extraction using clean_log from logs table

### Configuration Changes
- Updated incremental predicates to use `dynamic_range_predicate_custom`
- Added `modified_timestamp::DATE` to clustering keys
- Maintained unique key using tx_hash, destination_address, and source_address
- Maintained search optimization on tx_hash, destination_address, and source_address

### Query Changes
- Updated WHERE clause to filter on `action_name = 'FunctionCall'` and portalbridge patterns
- Updated partition_load_manual to include partition key calculation
- Added proper type casting for method_name and args from action_data
- Maintained separate CTEs for different bridge directions:
  * Outbound: vaa_withdraw with direct args parsing
  * Inbound: vaa_transfer with source chain ID from logs
- Improved log parsing efficiency:
  * Added upfront filtering in logs CTE to only fetch relevant transaction logs
  * Added log_index = 1 condition for precise source chain ID extraction
  * Used clean_log field for reliable regex extraction
- Enhanced source chain ID extraction by joining logs table with proper conditions

---

## silver__staking_pools_s3

### Major Changes
- **REQUIRES FULL REFRESH** - Model has been completely refactored to use `core__ez_actions`
- Refactored to use `core__ez_actions` instead of `silver__actions_events_function_call_s3` and `silver__transactions_final`
- Simplified query structure by removing unnecessary joins and CTEs
- Improved data quality by adding explicit receipt success checks

### Architecture Changes
- Removed dependency on deprecated `silver__actions_events_function_call_s3`
- Consolidated data sourcing into a single CTE from `core__ez_actions`
- Improved incremental processing with dynamic range predicate

### Column Changes
#### Removed
- No columns removed

#### Added
- `receipt_id` - Added for better transaction tracking and unique key generation

#### Modified
- Changed surrogate key generation to use `receipt_id` instead of `tx_hash`
- Updated source of `address` to use `receipt_receiver_id` for new pools
- Updated source of `owner` to use `tx_signer` for updated pools

### Configuration Changes
- Added dynamic range predicate for incremental processing
- Updated clustering keys to include both `block_timestamp::DATE` and `modified_timestamp::DATE`
- Changed unique key from `tx_hash` to `staking_pools_id`
- Added search optimization on `EQUALITY(tx_hash,receipt_id,owner,address)`

### Query Changes
- Simplified data sourcing by using `core__ez_actions` directly
- Added explicit success checks with `receipt_succeeded` and `tx_succeeded`
- Improved filtering by using native fields from `core__ez_actions`
- Maintained same business logic for identifying and processing staking pool transactions

--- 