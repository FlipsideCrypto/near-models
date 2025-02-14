-- depends on: {{ ref('streamline__get_chainhead') }}
{{ config(
    materialized = "view",
    tags = ['streamline_realtime', 'streamline_history', 'streamline_helper']
) }}

{% if execute %}
    {% set height = run_query("SELECT block_id from streamline.get_chainhead") %}
    {% set block_number = height.columns [0].values() [0] %}
{% else %}
    {% set block_number = 0 %}
{% endif %}

SELECT
    _id AS block_id
FROM
    {{ source(
        'crosschain_silver',
        'number_sequence'
    ) }}
WHERE
    _id <= {{ block_number }}
