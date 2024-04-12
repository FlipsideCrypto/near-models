{% macro dispatch_github_workflow(repo_name, workflow_name, gb_id) %}
    {% set context_query %}
        SET LIVEQUERY_CONTEXT = '{"userId":"{{ gb_id }}"}';
    {% endset %}
    {% do run_query(context_query) %}
    {% set query %}
        SELECT github_actions.workflow_dispatches('FlipsideCrypto', '{{ repo_name }}', '{{ workflow_name }}.yml', NULL)
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}