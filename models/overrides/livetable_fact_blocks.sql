-- depends_on: {{ ref('near_models','bronze__blocks') }}
-- depends_on: {{ ref('near_models','bronze__FR_blocks') }}
-- depends_on: {{ ref('near_models', 'silver__blocks_v2') }}
-- depends_on: {{ ref('near_models', 'silver__blocks_final') }}

SELECT * FROM {{ ref('near_models','core__fact_blocks')}}
