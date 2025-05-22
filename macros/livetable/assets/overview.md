## Near Livetable SPROC

### Overview
```
+-----------------+      +---------------------------------------------+      +---------------------------+
| Snowflake Task  |      | Stored Procedure (SP)                       |      |       _live.udf_api       |
| (Sched: 5 min)  |----->| SP_REFRESH_FACT_TRANSACTIONS_LIVE()         |      |---------------------------|
| QUERY_TAG: {...}|      |---------------------------------------------|      |                           |
+-----------------+      | 1. Check Schema/Table Exists (Idempotent)   |      |   [RPC call to Near Node] |
                         |    - CREATE SCHEMA IF NOT EXISTS ...        |      |   Quicknode Endpoint      |
                         |    - CREATE HYBRID TABLE IF NOT EXISTS ...  |      |                           |
                         |      (Incl. PRIMARY KEY)                    |      +---------------------------+
                         |                                             |                   ^
                         | 2. Get Chain Head                           |                   |
                         |    CALL _live.udf_api()---------------------+------------------>+
                         |          |                                  |                   |
                         |          |                                  |                   |
                         |          v                                  |                   |
                         |    [Near RPC API] <-------------------------|-------------------+
                         |          ^                                  |                   
                         |          | Returns Height                   |                   
                         |    height <-- _live.udf_api()               |                   
                         |                                             |                   
                         | 3. Calculate Start Block                    |      .-------------------------.
                         |    start_block = height - buffer            |      | Hybrid Table            |
                         |                                             |      | CORE_LIVE.              |
                         | 4. Call UDTF to Fetch Transactions          |      | FACT_TRANSACTIONS       |
                         |    rows <--                                 |      |-------------------------|
                         |       TABLE(livetable.tf_fact_tx(...)) ----+|      | - tx_hash (PK)          |
                         |             |                              ||      | - block_id              |
                         |             v UDTF Execution               ||      | - block_timestamp (*)   |
                         |        (Requires Owner Permissions)        ||      | - tx_signer             |
                         |        (Calls other UDFs for.              ||      | - tx_receiver           |
                         |         block/chunk/tx details             ||      | - ...                   |
                         |         internally)                        ||      | - _hybrid_updated_at    |
                         |             | mimic's gold fact_tx         ||      `-------------------------'
                         |             +------------------------------||                ^       |
                         |                                             |                |       | Step 6: Prune
                         | 5. MERGE Data into Hybrid Table             |--------------->|       | DELETE Rows
                         |    rows_merged = SQLROWCOUNT                |                |       | Older than 60m
                         |                                             |                |       | (based on block_ts)
                         | 7. Return Status String                     |                |       v
                         |    RETURN 'Fetched...Merged...Deleted...'   |                |  ............
                         +--------------------|------------------------+                |  : Old Data :
                                              |                                         |  :..........:
                                              | Status String                           |
                                              v                                         |
+------------------------+     +------------------------+                               |
| Snowflake Task History |<----| Task Record            |<------------------------------'
| (Logs Status, Return,  |     | (Captures SP Return)   |
|  Duration)             |     +------------------------+
+------------------------+
```