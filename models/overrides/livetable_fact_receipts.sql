-- depends_on: {{ ref('near_models','bronze__transactions') }}
-- depends_on: {{ ref('near_models','bronze__FR_transactions') }}
-- depends_on: {{ ref('near_models','bronze__blocks') }}
-- depends_on: {{ ref('near_models','bronze__FR_blocks') }}
-- depends_on: {{ ref('near_models', 'silver__blocks_v2') }}
-- depends_on: {{ ref('near_models', 'silver__blocks_final') }}
-- depends_on: {{ ref('near_models', 'silver__transactions_v2') }}
-- depends_on: {{ ref('near_models', 'silver__transactions_final') }}
-- depends_on: {{ ref('near_models', 'silver__receipts_final')}}

SELECT * FROM {{ ref('near_models','core__fact_receipts')}}
