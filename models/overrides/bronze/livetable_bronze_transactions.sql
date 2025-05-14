-- depends_on: {{ ref('near_models','bronze__transactions') }}
-- depends_on: {{ ref('near_models','bronze__FR_transactions') }}

SELECT * FROM {{ ref('near_models','bronze__FR_transactions')}}
