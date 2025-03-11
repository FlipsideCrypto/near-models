{{ config(
    severity = 'error',
    tags = ['gap_test']
) }}

WITH 
archive AS (
    select
        floor(block_id, -6) as block_group,
        count(distinct coalesce(receipt_object_id, receipt_id)) as receipt_count,
        count(distinct block_id) as block_count
    from {{ ref('silver__streamline_receipts_final') }}

    group by 1
),
destination AS (
    select
        floor(block_id, -6) as block_group,
        count(distinct receipt_id) as receipt_count,
        count(distinct block_id) as block_count
    from {{ ref('silver__receipts_final') }}

    group by 1
)
select
    archive.block_group,
    archive.receipt_count as receipt_ct_expected,
    archive.block_count as block_ct_expected,
    destination.receipt_count as receipt_ct_actual,
    destination.block_count as block_ct_actual
from archive
left join destination
on archive.block_group = destination.block_group
order by block_group
