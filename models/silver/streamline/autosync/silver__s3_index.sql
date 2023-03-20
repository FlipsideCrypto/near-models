-- TODO

{#
this is going to be like the nearblocks api model, where data ingestion comes from a macro
in that case, data is dumped into bronze (unmanaged by dbt) and the silver model is a standard incremental model, bringing in new data.
similar workflow here
 - query the bucket for file names in a partition
 - write that to a table here so it's indexed
 - reference that in this silver model
 - compare against ingested files outside the regular hourly workflow

need to remember 
 - add a buffer, files are constantly hitting the bucket via the sync and they'll get picked up by the hourly run
 - it should look BACK to get anything missed
 
does not need to be run very frequently, things are not missed often
    once a day?


#}