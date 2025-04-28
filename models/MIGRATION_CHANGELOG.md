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

## silver__token_transfer_deposit

### Major Changes
- Refactored to use `core__ez_actions` instead of `silver__actions_events_function_call_s3`
- Improved data quality by adding explicit receipt and transaction success checks

### Architecture Changes
- Removed dependency on deprecated `silver__actions_events_function_call_s3`
- Consolidated data sourcing into a single CTE from `core__ez_actions`
- Improved incremental processing with dynamic range predicate

### Column Changes
#### Removed
- `action_id` - Replaced with `receipt_id` and `action_index` for better granularity

#### Added
- `receipt_id` - Added for better transaction tracking
- `action_index` - Added to handle multiple actions within a receipt
- `modified_timestamp` - Added for incremental processing

### Configuration Changes
- Added dynamic range predicate for incremental processing
- Updated clustering keys to include both `block_timestamp::DATE` and `modified_timestamp::DATE`
- Changed surrogate key to use `receipt_id`, `action_index`, `predecessor_id`, `receiver_id`, and `amount_unadj`
- Added search optimization on `EQUALITY(tx_hash,receipt_id,predecessor_id,receiver_id)`

### Query Changes
- Simplified data sourcing by using `core__ez_actions` directly
- Added explicit success checks with `receipt_succeeded` and `tx_succeeded`
- Improved filtering by using native fields from `core__ez_actions`
- Maintained same business logic for identifying and processing token transfers

## silver__token_transfer_native

### Major Changes
- Refactored to use `core__ez_actions` instead of `silver__actions_events_s3`
- Improved data quality by adding explicit receipt and transaction success checks
- Enhanced tokens_burnt calculation using action_gas_price and receipt_gas_burnt

### Architecture Changes
- Removed dependency on deprecated `silver__actions_events_s3`
- Consolidated data sourcing into a single CTE from `core__ez_actions`
- Improved incremental processing with dynamic range predicate

### Column Changes
#### Removed
- `action_id` - Replaced with `receipt_id` and `action_index` for better granularity
- `_inserted_timestamp` - Deprecated column

#### Added
- `receipt_id` - Added for better transaction tracking
- `action_index` - Added to handle multiple actions within a receipt
- `modified_timestamp` - Added for incremental processing

#### Modified
- Changed source of gas fields to use native fields from `core__ez_actions`:
  * `gas_price` now uses `action_gas_price`
  * `gas_burnt` now uses `receipt_gas_burnt`
  * `tokens_burnt` calculation updated to use these new fields
- Updated source of account fields to use receipt-level fields:
  * `predecessor_id` from `receipt_predecessor_id`
  * `receiver_id` from `receipt_receiver_id`
  * `signer_id` from `receipt_signer_id`

### Configuration Changes
- Added dynamic range predicate for incremental processing
- Updated clustering keys to include both `block_timestamp::DATE` and `modified_timestamp::DATE`
- Changed surrogate key to use `receipt_id`, `action_index`, `predecessor_id`, `receiver_id`, and `amount_unadj`
- Added search optimization on `EQUALITY(tx_hash,receipt_id,predecessor_id,receiver_id)`

### Query Changes
- Simplified data sourcing by using `core__ez_actions` directly
- Added explicit success checks with `receipt_succeeded` and `tx_succeeded`
- Improved filtering by using native fields from `core__ez_actions`
- Enhanced tokens_burnt calculation using proper gas price and burnt values
- Maintained same business logic for identifying and processing native transfers

## silver__token_transfer_base

### Major Changes
- Refactored to use `core__ez_actions` and `silver__logs_s3` instead of `silver__actions_events_function_call_s3`
- Improved data quality by adding explicit receipt and transaction success checks
- Enhanced log validation by using `silver__logs_s3` directly

### Architecture Changes
- Split data sourcing into two CTEs:
  1. `base_actions` CTE from `core__ez_actions` for function call data
  2. `logs_check` CTE from `silver__logs_s3` for log validation
- Improved incremental processing with dynamic range predicate
- Changed log validation to use clean_log from `silver__logs_s3`

### Column Changes
#### Removed
- `action_id` - Replaced with `receipt_id` and `action_index` for better granularity
- `_inserted_timestamp` - Deprecated column
- Removed dependency on deprecated `logs` column from actions table

#### Added
- `receipt_id` - Added for better transaction tracking
- `action_index` - Added to handle multiple actions within a receipt (always 0 for this model)

#### Modified
- Changed source of account fields to use receipt-level fields:
  * `predecessor_id` from `receipt_predecessor_id`
  * `receiver_id` from `receipt_receiver_id`
  * `signer_id` from `receipt_signer_id`
- Changed `method_name` to parse from `action_data :method_name`
- Changed `deposit` to parse from `action_data :deposit`

### Configuration Changes
- Added dynamic range predicate for incremental processing
- Updated clustering keys to include both `block_timestamp::DATE` and `modified_timestamp::DATE`
- Changed surrogate key to use `receipt_id` and `action_index`
- Added search optimization on `EQUALITY(tx_hash,receipt_id,predecessor_id,receiver_id)`

### Query Changes
- Simplified data sourcing by using `core__ez_actions` directly
- Added explicit success checks with `receipt_succeeded` and `tx_succeeded`
- Improved filtering by using native fields from `core__ez_actions`
- Changed log validation to use proper join with `silver__logs_s3`
- Maintained same business logic for identifying first actions with logs

## silver__token_transfer_ft_transfers_event

### Major Changes
- Model has been refactored to use `silver__logs_s3` directly instead of `token_transfer_base`
- Improved log handling by directly filtering for standard EVENT_JSON logs with ft_transfer events
- Maintains same business logic for capturing FT transfer events

### Architecture Changes
- Removed dependency on deprecated `silver__token_transfer_base`
- Now sources data directly from `silver__logs_s3`
- Added upfront filtering for standard EVENT_JSON logs and ft_transfer events
- Improved event indexing using log_index

### Column Changes
- Added `receipt_id` for better transaction tracking
- Added `predecessor_id` and `signer_id` from logs table
- Renamed `rn` to `event_index` for clarity
- Changed `contract_address` to come directly from `receiver_id` in logs
- Removed `action_id` (replaced by receipt_id)

### Configuration Changes
- Added `dynamic_range_predicate_custom` for incremental processing
- Added `modified_timestamp::DATE` to clustering keys
- Added search optimization on key fields (tx_hash, receipt_id, contract_address, from_address, to_address)
- Changed surrogate key to use receipt_id instead of tx_hash for better uniqueness

### Query Changes
- Simplified log parsing by using `clean_log` directly from logs table
- Added direct filtering for ft_transfer events in logs CTE
- Maintained same business logic for amount validation and address extraction
- Enhanced event indexing to properly track multiple events within a transaction

### Testing Changes
- Added comprehensive column-level tests:
  * Type validation for all fields
  * Regex validation for NEAR addresses
  * Amount validation to ensure positive values
  * Not-null constraints where appropriate
- Added unique combination test across key fields

## silver__token_transfer_liquidity

⚠️ **REQUIRES FULL REFRESH** - Fixed from_address handling to to use predecessor_id

### Major Changes
- Model has been refactored to use `silver__logs_s3` directly instead of `token_transfer_base`
- Improved log handling by directly filtering for 'Liquidity added' events
- Fixed from_address to properly set NULL values for liquidity additions
- Maintains same business logic for capturing liquidity addition events

### Architecture Changes
- Removed dependency on deprecated `silver__token_transfer_base`
- Now sources data directly from `silver__logs_s3`
- Added upfront filtering for liquidity logs with log_index = 0
- Improved event indexing using log_index

### Column Changes
- Added `receipt_id` for better transaction tracking
- Added `predecessor_id` and `signer_id` from logs table
- Renamed `rn` to `event_index` for clarity
- Changed `contract_address` to come directly from regex on log value
- Removed `action_id` (replaced by receipt_id)
- Fixed `from_address` to consistently use NULL for liquidity additions

### Configuration Changes
- Added `dynamic_range_predicate_custom` for incremental processing
- Added `modified_timestamp::DATE` to clustering keys
- Added search optimization on key fields (tx_hash, receipt_id, contract_address, to_address)
- Changed surrogate key to use receipt_id instead of tx_hash for better uniqueness

### Query Changes
- Simplified log parsing by using `clean_log` directly from logs table
- Added direct filtering for liquidity events in logs CTE
- Improved log value extraction by removing unnecessary array access
- Enhanced event indexing to properly track multiple events within a transaction
- Maintained same regex pattern for extracting contract address and amount

### Testing Changes
- Added comprehensive column-level tests:
  * Type validation for all fields
  * Regex validation for NEAR addresses
  * Amount validation to ensure positive values
  * Not-null constraints where appropriate
  * Value set validation for memo field

## silver__logs_s3

### Major Changes
- **REQUIRES MANUAL MIGRATION** - Column `receipt_object_id` is being removed in favor of just `receipt_id`
- Migration steps:
  1. Verify no null values in receipt_id: `SELECT COUNT(*) FROM silver__logs_s3 where receipt_id is null;`
  2. Execute ALTER TABLE command: `ALTER TABLE silver__logs_s3 RENAME COLUMN receipt_object_id TO receipt_id;`

### Architecture Changes
- Simplified column naming by removing redundant `receipt_object_id` column
- All references to `receipt_object_id` in downstream models already use `receipt_id` or alias it to `receipt_id`

### Column Changes
#### Removed
- `receipt_object_id` - Redundant column, functionality preserved through `receipt_id`

### Configuration Changes
- Updated search optimization to use `receipt_id` instead of `receipt_object_id`
- No changes to unique key or clustering configuration

### Query Changes
- No changes to core query logic
- Column renaming handled through ALTER TABLE command
- All downstream models already compatible with `receipt_id`

### Downstream Impact
- `defi__fact_intents.sql` - Currently aliases `receipt_object_id` to `receipt_id`, will continue to work after migration
- All other models using `silver__logs_s3` already reference `receipt_id` directly
- No changes needed in downstream models as they are already aligned with the new structure

### silver__token_transfer_mints

**Major Changes**
- Refactored model to use `silver__logs_s3` directly instead of `token_transfer_base`
- Improved handling of mint events by directly filtering for 'ft_mint' log type
- Added `event_index` to maintain event order within transactions
- Full refresh required (FR) due to improved log handling and event ordering

**Architecture**
- Data sourcing split into two main CTEs:
  - `ft_mint_logs`: Filters relevant mint events from `silver__logs_s3`
  - `ft_mints_final`: Processes and formats mint events with proper field extraction

**Column Changes**
- Added:
  - `event_index`: Tracks event order within transactions
  - `receipt_id`: Links to originating receipt
- Modified:
  - `mint_id`: Now generated using consistent surrogate key pattern
  - All timestamp fields now use TIMESTAMP_NTZ type

**Configuration Changes**
- Updated `incremental_predicates` to use dynamic range predicate
- Added `modified_timestamp::DATE` to clustering keys
- Maintained `merge` strategy for incremental processing
- Optimized merge exclude columns for better performance

**Query Changes**
- Enhanced log filtering to ensure only successful mint events are processed
- Improved amount validation to ensure positive values only
- Added proper handling of NEAR account formats in address fields
- Optimized field extraction from EVENT_JSON

**Testing**
- Added comprehensive column tests including:
  - Data type validations
  - Not null constraints where appropriate
  - NEAR account format validation for addresses
  - Positive amount validation
  - Uniqueness check for mint_id
- Enhanced model description to better reflect its purpose and data sources

### silver__token_transfers_complete
`2024-03-XX`
### 🏗️ Architecture Changes
- Improved incremental logic to prevent data gaps:
  - Added execute block to calculate minimum block date and maximum modified timestamp
  - Split filtering strategy: block_timestamp at CTE level, modified_timestamp after UNION ALL
  - Added final CTE for cleaner deduplication with QUALIFY clause
- Updated column names to align with new standards:
  - Replaced `action_id` with `receipt_id`
  - Replaced `rn` with `event_index`
- Added comprehensive tests in YAML file for all columns
- Added type validation tests for timestamps and numeric fields
- Added value validation for transfer_type field

### 🔄 Query Changes
- Optimized incremental processing by:
  - Using date-based filtering at source CTEs
  - Applying modified_timestamp filter after UNION ALL
  - Moving QUALIFY clause to final SELECT
- Standardized WHERE clause structure across all CTEs
- Added explicit column list in final_transfers CTE

### 🎯 Functionality Changes
- Maintained all existing transfer types and sources
- Enhanced surrogate key generation using new column names
- Added search optimization on key columns including receipt_id

### silver__token_transfer_orders

**Major Changes**
- Refactored model to use `silver__logs_s3` directly instead of `token_transfer_base`
- Improved handling of order events by directly filtering for 'order_added' log type
- Added `event_index` to maintain event order within transactions
- Full refresh required (FR) due to improved log handling and event ordering

**Architecture**
- Data sourcing split into two main CTEs:
  - `order_logs`: Filters relevant order events from `silver__logs_s3`
  - `orders_final`: Processes and formats order events with proper field extraction

**Column Changes**
- Added:
  - `event_index`: Tracks event order within transactions
  - `receipt_id`: Links to originating receipt
  - `predecessor_id`: Links to transaction originator
  - `signer_id`: Links to transaction signer
- Modified:
  - `transfers_orders_id`: Now generated using consistent surrogate key pattern
  - All timestamp fields now use TIMESTAMP_NTZ type
- Removed:
  - `action_id`: Replaced by receipt_id and event_index
  - `_inserted_timestamp`: Deprecated column

**Configuration Changes**
- Updated `incremental_predicates` to use dynamic range predicate
- Added `modified_timestamp::DATE` to clustering keys
- Maintained `merge` strategy for incremental processing
- Added search optimization on key fields (tx_hash, receipt_id, contract_address, from_address, to_address)

**Query Changes**
- Enhanced log filtering to ensure only successful order events are processed
- Improved amount validation to ensure positive values only
- Added proper handling of NEAR account formats in address fields
- Optimized field extraction from EVENT_JSON
- Simplified log parsing by using clean_log directly

**Testing**
- Added comprehensive column tests including:
  - Data type validations
  - Not null constraints where appropriate
  - NEAR account format validation for addresses
  - Positive amount validation
  - Value set validation for memo field
  - Uniqueness check for transfers_orders_id
- Enhanced model description to better reflect its purpose and data sources

### silver__token_transfer_ft_transfers_method

**Major Changes**
- Refactored model to use both `core__ez_actions` and `silver__logs_s3` directly
- Improved handling of ft_transfer method calls by properly joining actions with their logs
- Added `event_index` to maintain action and log order within transactions
- Changed from flattening logs array to proper join with logs table

**Architecture**
- Split data sourcing into three CTEs:
  1. `ft_transfer_actions`: Filters relevant ft_transfer actions from `core__ez_actions`
  2. `ft_transfer_logs`: Joins actions with their logs from `silver__logs_s3`
  3. `ft_transfers_final`: Processes and formats transfer events with proper field extraction
- Improved log handling by using clean_log field directly
- Maintained regex-based field extraction from logs

**Column Changes**
- Added:
  - `event_index`: Tracks combined action and log order within transactions
  - `receipt_id`: Links to originating receipt
  - `predecessor_id`: Links to transaction originator
  - `signer_id`: Links to transaction signer
- Modified:
  - `transfers_id`: Now generated using consistent surrogate key pattern
  - `from_address`: Now extracted from clean_log using regex
  - `to_address`: Now extracted from clean_log using regex
  - `amount_unadj`: Now extracted from clean_log using regex
  - All timestamp fields now use TIMESTAMP_NTZ type
- Removed:
  - `action_id`: Replaced by receipt_id and event_index
  - `_inserted_timestamp`: Deprecated column

**Configuration Changes**
- Updated `incremental_predicates` to use dynamic range predicate
- Added `modified_timestamp::DATE` to clustering keys
- Changed from 'delete+insert' to 'merge' strategy for incremental processing
- Added search optimization on key fields (tx_hash, receipt_id, contract_address, from_address, to_address)

**Query Changes**
- Enhanced filtering to ensure only successful ft_transfer calls are processed
- Improved log handling by properly joining actions with their logs
- Added proper handling of NEAR account formats in address fields
- Maintained regex-based field extraction for consistency
- Added validation for required log fields (from, to, amount)

**Testing**
- Added comprehensive column tests including:
  - Data type validations
  - Not null constraints where appropriate
  - NEAR account format validation for addresses
  - Positive amount validation
  - Uniqueness check for transfers_id
- Enhanced model description to better reflect its purpose and data sources

### silver__atlas_supply_daily_lockup_locked_balances
**Date**: 2024-03-XX

#### Changes
- Architecture:
  - Replaced `silver__actions_events_function_call_s3` with `core__ez_actions`
  - Updated column mappings to match new schema:
    - `receipt_receiver_id` → `receiver_id`
    - `action_data :method_name` → `method_name`
    - `action_data :args` → `args`
  - Added `action_index` to function call CTE for better event tracking
  - Added `receipt_succeeded` field from core__ez_actions

#### Impact
- No functional changes to the model's logic or output
- Maintains same lockup contract tracking and vesting calculations
- Preserves all existing downstream dependencies

### silver__nft_paras_sales
**Date**: 2024-03
**Changes**:
- Architecture:
  - Migrated from `silver__actions_events_function_call_s3` to `core__ez_actions`
  - Improved log handling by using `receipt_status_value` directly from `core__ez_actions`
  - Updated unique key to use `receipt_id` and `action_index`
  - Added dynamic range predicate for incremental updates
  - Added `modified_timestamp::DATE` to clustering keys

- Query Changes:
  - Optimized data sourcing:
    - Main CTE filters directly from `core__ez_actions` for both marketplace actions and NFT transfer payouts
    - Added filtering for both receiver_id and predecessor_id to capture all Paras marketplace interactions
  - Improved royalty handling by using `receipt_status_value` directly from actions
  - Maintained complex COALESCE logic for handling various sale types (regular sales and offers)

- Column Changes:
  - Replaced `action_id` with `receipt_id` and `action_index`
  - Added direct access to `receipt_status_value` for royalty extraction
  - Maintained all existing business logic for:
    - Price calculations using various args patterns
    - Royalty extraction from SuccessValue
    - Platform fee calculations (2% of sale price)

- Configuration:
  - Updated incremental predicates to use dynamic range on `block_timestamp::date`
  - Modified clustering strategy to include both timestamp fields
  - Preserved existing tag configuration

### silver__nft_other_sales
**Date**: 2024-03
**Changes**:
- Architecture:
  - Migrated from `silver__actions_events_function_call_s3` to `core__ez_actions` for all action-based sales
  - Migrated all log handling to `silver__logs_s3` with direct joins and event parsing
  - Maintained separate CTEs for Mitte marketplace, using logs for event extraction
  - Updated unique key to use `nft_other_sales_id` generated from `receipt_id` and `action_index`
  - Added dynamic range predicate for incremental updates
  - Added `modified_timestamp::DATE` to clustering keys

- Query Changes:
  - Split data sourcing into three CTEs:
    - `actions`: Filters directly from `core__ez_actions` for all supported marketplaces
    - `logs`: Processes event logs from `silver__logs_s3` with proper join conditions
    - `mitte_logs`: Handles Mitte-specific event parsing with optimized log filtering
  - Preserved all COALESCE and CASE logic for extracting:
    - Seller address from various args patterns
    - Buyer address from direct and offer-based purchases
    - NFT contract and token identification
    - Price calculations with proper decimal handling
  - Improved marketplace-specific logic:
    - Apollo42, TradePort, UniqArt, L2E, FewAndFar: Direct args parsing
    - Mitte: Enhanced log parsing with proper order array handling
  - Added proper join conditions between actions and logs using tx_hash and receipt_id

- Column Changes:
  - Replaced `action_id` with `receipt_id` and `action_index`
  - Updated source fields to use receipt-level identifiers:
    - `receipt_receiver_id` for platform_address
    - `receipt_signer_id` for transaction attribution
  - Maintained all business logic for:
    - Price calculations (division by 1e24)
    - Platform name mapping
    - NFT contract and token ID extraction
  - Added proper type casting for all extracted fields

- Configuration:
  - Updated incremental predicates to use dynamic range on `block_timestamp::date`
  - Modified clustering strategy to include both timestamp fields
  - Added search optimization on key fields
  - Preserved existing tag configuration
  - Updated merge strategy for better incremental processing

--- 