{#-
  Tester at alle kolonner i en modell ikke er null, med mulighet for å ekskludere spesifikke kolonner.
  Dette er en mer effektiv måte å sjekke for null-verdier på tvers av flere kolonner sammenlignet med
  å opprette individuelle not_null-tester for hver kolonne.

  Argumenter:
    - model: Modellen som skal testes.
    - exceptions: En liste over kolonnenavn som skal ekskluderes fra testen. Standard er ingen unntak.
    - columns: En liste over kolonne navn som skal sjekkes. Hvis ikke spesifisert, sjekkes alle kolonner i modellen.
        NB: Når man bruker denne testen på en source må kolonnen spesifiseres når man ikke vil sjekke alle kolonner i kilden,
        som kan inkludere flere kolonner som er relevant for staging osv.
-#}
{%- test UTILS__columns_are_not_null(model, columns=none, exceptions=none) -%}
    {%- if not execute -%}
        {{ return([0]) }}
    {%- endif -%}

    {%- set exceptions = exceptions | map("upper") | list if exceptions is iterable and exceptions is not string else [] -%}
    {%- set columns = columns | map("upper") | list if columns is iterable and columns is not string else [] -%}

    {#-
        Følgende seksjon er en workaround for å kunne spesifisere "where" konfigurasjonen
        I så fall er "model" ikke lengre en relation som man kan sende inn til adapter.get_columns_in_relation.
    -#}
    {%- if not columns -%}
        {%- set refs = config.model.refs -%}
        {%- set sources = config.model.sources -%}
        {%-  if refs | count == 1 -%}
            {%- set test_target_model = ref(refs[0].name) -%}
        {%- elif sources | count == 1 -%}
            {#- sources = list of [schema, table] -#}
            {# "source" leverer et modell objekt tilbake #}
            {%- set test_target_model = source(sources[0][0], sources[0][1]) -%}
        {%- else -%}
            {{ exceptions.raise_compiler_error("Multiple or no sources or references in test") }}
        {%- endif -%}

        {#- Hent alle kolonnenavn fra databasen -#}
        {%- set all_columns = adapter.get_columns_in_relation(test_target_model) | map(attribute="name") | map("upper") | list -%}

    {%- else -%}
        {%- set all_columns = columns -%}
    {%- endif -%}

    {#- Fjern unntak -#}
    {%- set columns_to_check = [] -%}
    {%- for column in all_columns -%}
        {%- if column not in exceptions -%}
            {%- do columns_to_check.append(column) -%}
        {%- endif -%}
    {%- endfor -%}

    {#- Bygg sql spørring som returnerer en rad per feilet sjekk-#}
    with source_data as (
        select * from {{ model }}
    )

    select 1
    from source_data
    where
        {%- for column in columns_to_check %}
            {{ column }} is null
            {%- if not loop.last %} or {% endif %}
        {%- endfor -%}

{%- endtest -%}


{%- macro UTILS__lokal_dato_til_utc(sql_uttrykk_streng) -%}
cast(from_tz(cast({{ sql_uttrykk_streng }} as timestamp), 'Europe/Oslo') at time zone 'UTC' as date)
{%- endmacro -%}


{%- macro UTILS__lokal_timestamp_til_utc(sql_uttrykk_streng) -%}
cast(from_tz({{ sql_uttrykk_streng }}, 'Europe/Oslo') at time zone 'UTC' as timestamp)
{%- endmacro -%}