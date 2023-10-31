-- from aurora
select
near.core.fact_receipts.status_value,
near.core.fact_transactions.tx_status,
near.core.fact_transactions.tx_hash,
LEFT(RIGHT(near.core.fact_receipts.logs[0], 43), 42) as th
from near.core.fact_transactions 
inner join near.core.fact_receipts using(tx_hash)
where near.core.fact_transactions.tx_signer = 'relay.aurora'
and tx_status = 'Fail'
and block_timestamp >= '2023-09-15'
and near.core.fact_receipts.status_value NOT LIKE '%ERR_INCORRECT_NONCE%'
limit 100;

-- from modefi
select *,
    tx:execution_outcome :outcome :status :Failure IS NOT NULL
from near.silver.streamline_transactions_final
where tx_hash in ('F8aLtLN3MQ87kcWfsW9GWzowj1cGUondMT8xyZhY1kPr', '5xec7bgSw45zUzQws3rufYYu3bpNB6gxVtyNy8teew7i')
and _partition_by_block_number between 65000000 and 76000000
;

select * from near.silver.streamline_receipts_final
where tx_hash in ('F8aLtLN3MQ87kcWfsW9GWzowj1cGUondMT8xyZhY1kPr', '5xec7bgSw45zUzQws3rufYYu3bpNB6gxVtyNy8teew7i')
and _partition_by_block_number between 65000000 and 76000000
;

select count(1) from near.silver.streamline_transactions_final;
-- 439,326,753

select count(distinct tx_hash) from near.silver.streamline_receipts_final
where not receipt_succeeded;
-- 57,746,460
-- so, 58mm txs with a failed receipt of some kind. Not all of these are fully failed txs
