{% macro dispatch_github_workflow(repo_name, workflow_name) %}
    {% set context_query %}
        SET LIVEQUERY_CONTEXT = '{"userId":"{{ var('GB_ID') }}"}';
    {% endset %}
    {% do run_query(context_query) %}
    {% set query %}
        SELECT github_actions.workflow_dispatches('FlipsideCrypto', '{{ repo_name }}', '{{ workflow_name }}.yml', NULL)
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}