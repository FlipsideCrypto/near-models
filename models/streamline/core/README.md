# Streamline Core Models

This readme is to persist some notes throughout the Streamline migration for ongoing and later steps.

## TODOs

### Data Quality
 - receipt map gaps-- ran a test on the full data and logged it to the schema `near.tests_full`. Replay those block heights to get the mapped transactions. No issues with them slotting in incrementally.

### Deprecations
 - noted several models with "-- TODO slated for deprecation and drop" that are deprecated. Delete & drop, accordingly

### Observability Models 
 - update refs to fact models once tables
 - blocks and chunks completeness = good
 - logs completeness = maybe not the right item. Receipts + Actions (+ Logs ?)

### Stats Models
 - update refs to fact models once tables
