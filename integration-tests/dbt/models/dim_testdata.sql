{#- Generic SCD test model. Shape and column naming are driven entirely by environment variables so a
    single model can cover every combination of user configuration.

    USE_CUSTOM_NAMES  "true" renames every configurable column, exercising the config overrides.
    USE_EXISTING_PK   "true" supplies the primary key from the source, so the macro must adopt it
                      instead of generating one.
    EXCLUDE_COLUMNS   comma separated data columns to leave out of the select, which is how a
                      schema change is provoked between two runs.
    USE_WITH_CLAUSE   "true" makes the model a with query, which the macro must wrap without
                      nesting it inside a with clause of its own (ORA-32034).

    NB: env_var always returns a string, so compare explicitly. A bare {% if env_var(...) %} is true
        for any non-empty value, including "false". -#}
{%- set use_custom_names = env_var('USE_CUSTOM_NAMES', 'false') == 'true' -%}
{%- set use_existing_pk = env_var('USE_EXISTING_PK', 'false') == 'true' -%}
{%- set exclude = env_var('EXCLUDE_COLUMNS', '').split(',') | reject('equalto', '') | list -%}
{%- set use_with_clause = env_var('USE_WITH_CLAUSE', 'false') == 'true' -%}

{%- if use_custom_names -%}
{{ config(
    primary_key="pk_test",
    changed_at="endret",
    created_at="opprettet",
    valid_from="gyldig_fra_og_med",
    valid_to="gyldig_til",
    valid_flag="gyldig_naa",
    updated_at="oppdatert",
    loaded_at="lastet"
) }}
{%- endif -%}

{#- mirrors SCD__validate_config, where primary_key defaults to "pk_" ~ model name -#}
{%- set primary_key = "pk_test" if use_custom_names else "pk_dim_testdata" -%}
{%- set changed_at = "endret" if use_custom_names else "oppdatert_tid_kilde" -%}
{%- set created_at = "opprettet" if use_custom_names else "opprettet_tid_kilde" -%}

{#- the data columns keep their source names under both naming schemes, so SCD_KEY and SCD_HASH
    stay valid whichever naming is in play -#}
{%- set data_columns = ['kode1', 'kode2', 'navn1', 'navn2'] | reject('in', exclude) | list -%}

{%- if use_with_clause %}
with DBT_TEST_MODEL_CTE as (
{%- endif %}
select
    {% if use_existing_pk %}pk as {{ primary_key }}
    , {% endif %}
    {%- for col in data_columns %}
    {{ col }}
    , {% endfor %}
    tid1 as {{ changed_at }}
    , tid2 as {{ created_at }}
from
    {{ source("dbtuser", "testdata") }}
{%- if use_with_clause %}
)
select * from DBT_TEST_MODEL_CTE
{%- endif %}
