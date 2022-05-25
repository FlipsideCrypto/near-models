{% macro grant_data_share() %}
    {% if target.schema == 'PROD' %}
        GRANT
    SELECT
        ON ALL tables IN schema "NEAR"."PROD" TO SHARE "NEAR_MDAO";
    {% else %}
    SELECT
        1;
    {% endif %}
{% endmacro %}
