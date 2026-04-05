/*
  Custom generate_schema_name macro.

  Behaviour:
  - In production (target.name == 'prod'): use the schema defined in dbt_project.yml
    (e.g. 'core', 'staging', 'finance'). This creates clean shared schemas.
  - In development / CI: prefix with the user's personal schema
    (e.g. 'dbt_alice_core', 'dbt_ci_staging') to avoid stepping on each other.

  Usage: this macro is called automatically by dbt when resolving schema names.
  You do not need to call it manually.

  Reference: https://docs.getdbt.com/docs/build/custom-schemas
*/

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if target.name == 'prod' -%}

        {#- Production: use the configured schema as-is (no prefix) -#}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}

    {%- else -%}

        {#- Dev / CI: prefix the custom schema with the personal target schema -#}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ default_schema }}_{{ custom_schema_name | trim }}
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}
