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