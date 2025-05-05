-- depends_on: {{ ref('near_models','bronze__transactions') }}
-- depends_on: {{ ref('near_models','bronze__FR_transactions') }}
-- depends_on: {{ ref('near_models', 'silver__transactions_v2') }}
-- depends_on: {{ ref('near_models', 'silver__transactions_final') }}
-- depends_on: {{ ref('near_models', 'core__fact_transactions') }}

SELECT * FROM {{ ref('near_models','core__fact_transactions_live')}}
