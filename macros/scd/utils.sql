
{% macro SCD__is_list(x) %}
    {% do return(x is sequence and x is not mapping and x is not string) %}
{% endmacro %}


{% macro SCD__add_error_msg(ns, category, msg) %}
    {% if not category in ns.errors %}
        {% do ns.errors.update({category: []}) %}
    {% endif %}
    {% do ns.errors[category].append(msg) %}
{% endmacro %}


{% macro SCD__validate_config() %}
{# --------------------------------------------------------------------
    input: 
        config: model config (implicit)
    returns:
        namespace
    description:
        Parses and validates model config for scd materialization settings
        errors are gathered in ns.errors dict
#}
    {% set ns = namespace() %}

    {% set ns.errors = {} %} 

    {% set ns.primary_key = config.get("primary_key", "pk_" ~ config.model.name).lower() %}
    {% if not ns.primary_key is string %}
        {% do dbt_dvh_macros.SCD__add_error_msg(ns, "config", "primary_key property must be string") %}
    {% endif %}

    {% set tmp = config.get("scd_key", none) %}
    {% set ns.scd_key_columns = [tmp] if tmp is string else tmp %}
    {% if not dbt_dvh_macros.SCD__is_list(ns.scd_key_columns) %}
        {% do dbt_dvh_macros.SCD__add_error_msg(ns, "config", "scd_key property missing or must be string or list of strings") %}
    {% endif %}

    {% set tmp = config.get("scd_hash", []) %}
    {% set ns.scd_hash_columns = [tmp] if tmp is string else tmp %}
    {% if not dbt_dvh_macros.SCD__is_list(ns.scd_hash_columns) %}
        {% do dbt_dvh_macros.SCD__add_error_msg(ns, "config", "scd_hash property must be string or list of strings") %}
    {% endif %}

    {% set ns.created_at = config.get("created_at", "opprettet_tid_kilde").lower() %}
    {% if not ns.created_at is string %}
        {% do dbt_dvh_macros.SCD__add_error_msg(ns, "config", "created_at property must be string") %}
    {% endif %}

    {% set ns.changed_at = config.get("changed_at", "oppdatert_tid_kilde").lower() %}
    {% if not ns.changed_at is string %}
        {% do dbt_dvh_macros.SCD__add_error_msg(ns, "config", "changed_at property must be string") %}
    {% endif %}

    {% set ns.valid_from = config.get("valid_from", "gyldig_fom_tid").lower() %}
    {% if not ns.valid_from is string %}
        {% do dbt_dvh_macros.SCD__add_error_msg(ns, "config", "valid_from property must be string") %}
    {% endif %}

    {% set ns.valid_to = config.get("valid_to", "gyldig_til_tid").lower() %}
    {% if not ns.valid_to is string %}
        {% do dbt_dvh_macros.SCD__add_error_msg(ns, "config", "valid_to property must be string") %}
    {% endif %}

    {% set ns.valid_flag = config.get("valid_flag", "gyldig_flagg").lower() %}
    {% if not ns.valid_flag is string %}
        {% do dbt_dvh_macros.SCD__add_error_msg(ns, "config", "valid_flag property must be string") %}
    {% endif %}

    {% set ns.loaded_at = config.get("loaded_at", "lastet_dato").lower() %}
    {% if not ns.loaded_at is string %}
        {% do dbt_dvh_macros.SCD__add_error_msg(ns, "config", "loaded_at property must be string") %}
    {% endif %}

    {% set ns.updated_at = config.get("updated_at", "oppdatert_dato").lower() %}
    {% if not ns.updated_at is string %}
        {% do dbt_dvh_macros.SCD__add_error_msg(ns, "config", "updated_at property must be string") %}
    {% endif %}

    {% set ns.valid_from_default = "to_date('01.01.1900', 'dd.mm.yyyy')" %}
    {% set ns.valid_to_default = "to_date('31.12.9999', 'dd.mm.yyyy')" %}

    {% set ns.filter_mode = config.get("filter_mode", none) %}
    {% if not ns.filter_mode in ["changed_at", "changed_at_per_scd_key", "scd_key"] %}
        {% do dbt_dvh_macros.SCD__add_error_msg(ns, "config", "unrecognized or missing filter_mode") %}
    {% endif %}

    {% set ns.scd_type = config.get("scd_type", none)%}
    {% if not ns.scd_type in [0, 1, 2] %}
        {% do dbt_dvh_macros.SCD__add_error_msg(ns, "config", "unrecognized or missing scd_type") %}
    {% endif %}

    {% set tmp = config.get("schema_changes_enabled", []) %}
    {% if not dbt_dvh_macros.SCD__is_list(tmp) %}
        {% do dbt_dvh_macros.SCD__add_error_msg(ns, "config", "schema_changes_enabled must be list of strings") %}
    {% else %}
        {% for enabled_change in tmp %}
            {% if enabled_change not in ["append", "remove", "expand", "morph"] %}
                {% do dbt_dvh_macros.SCD__add_error_msg(ns, "config", "unknown schema change") %}
            {% endif %}
        {% endfor %}
    {% endif %}
    {% set ns.append = "append" in tmp %}
    {% set ns.remove = "remove" in tmp %}
    {% set ns.expand = "expand" in tmp %}
    {% set ns.morph = "morph" in tmp %}

    {% do return(ns) %}

{% endmacro %}


{% macro SCD__validate_source_columns_against_config(ns) %}
{# --------------------------------------------------------------------
    input: 
        ns: scd namespace settings
        note: ns.source_columns and ns.target_relation must be set in advance
    returns:
        nothing
    description:
        Parses and validates model config for scd materialization settings
        errors are gathered in ns.errors dict
#}

    {% set source_columns_names = ns.source_columns | map(attribute="name") | map("lower") | list %}

    {% set must_exist_columns = 
        ns.scd_key_columns
        + ns.scd_hash_columns
        + [ns.created_at, ns.changed_at]
    %}
    {% for col in must_exist_columns %}
        {% if col not in source_columns_names %}
            {% do dbt_dvh_macros.SCD__add_error_msg(ns, "model", col ~ " missing from select")%}
        {% endif %}
    {% endfor %}

    {% set must_not_exist_columns = [ns.valid_from, ns.valid_to, ns.valid_flag] %}
    {% for col in must_not_exist_columns %}
        {% if col in source_columns_names %}
            {% do dbt_dvh_macros.SCD__add_error_msg(ns, "model", col ~ " not allowed in select ")%}
        {% endif %}
    {% endfor %}

    {#  data_columns are the columns which are updated in SCD-1 when a matching row is found.
        At the moment they are only used by SCD-1.
        They include all columns except those that do not make sense to update
        NB: valid_from/to/flag do not make sense to update in SCD-1,
            and are not allowed in the model select.
        NBNB: In the future we may decide to support mixing SCD-0/1 on a per-column basis
            similar to DBT incremental model's merge_update_columns.
    #}
    {% set ns.data_columns = source_columns_names
        | reject("in", ns.scd_key_columns + [ns.primary_key, ns.created_at, ns.loaded_at])
        | list 
    %}
    {% set ns.generate_primary_key = ns.primary_key not in source_columns_names %}
    {% set ns.generate_updated_at = ns.updated_at not in source_columns_names %}
    {% set ns.generate_loaded_at = ns.loaded_at not in source_columns_names %}

    {% set ns.generated_columns = must_not_exist_columns %}
    {% if ns.generate_primary_key %} {% do ns.generated_columns.append(ns.primary_key) %} {% endif %}
    {% if ns.generate_updated_at %} {% do ns.generated_columns.append(ns.updated_at) %} {% endif %}
    {% if ns.generate_loaded_at %} {% do ns.generated_columns.append(ns.loaded_at) %} {% endif %}

    {% if ns.generate_primary_key %}
        {% set ns.expression_primary_key %}
            rownum + (
                select nvl(greatest(max(DBT_INTERNAL_GENERATED_PK.{{ ns.primary_key }}), 0), 0)
                from {{ ns.target_relation }} DBT_INTERNAL_GENERATED_PK
            )
        {% endset %}
    {% endif %}

    {% for col in ns.source_columns %}
        {% if col.name | lower == ns.changed_at %}
            {% set ns.changed_at_data_type = col.data_type %}
            {% break %}
        {% endif %}
    {% endfor %}
    {% if not ns.changed_at_data_type %}
        {% do dbt_dvh_macros.SCD__add_error_msg(ns, "internal", "unable to determine changed_at datatype")%}
    {% endif %}

{% endmacro %}


{% macro SCD__get_scd_model_source_insert_sql(ns, ignore_filter, sql) %}
{# --------------------------------------------------------------------
    # This used to be SCD__wrap_scd_model_select_with_filter
    #   changed to insert version
    #   since we cant get precise datatypes with get columns in query
    input:
        ns (namespace from SCD__get_scd_settings after validation)
        ignore_filter, bool, if true filter is ignored (full refresh for example)
        sql (model select)
        NOTE: ns.source_columns and ns.target_relation must be set
            and schema changes processed
    returns:
        modified sql, dependent on config

#}

    {% set source_columns_names = ns.source_columns | map(attribute="name") | map("lower") | list %}

    insert into {{ ns.source_relation }} (
        {% for col in source_columns_names %}
            {% if not loop.first %} , {% endif %} {{ col }}
        {% endfor %}
    )
    select
        {% for col in source_columns_names %}
            {% if not loop.first %} , {% endif %} DBT_INTERNAL_WRAP_SRC.{{ col }}
        {% endfor %}
    from (
        {{ sql }}
    ) DBT_INTERNAL_WRAP_SRC
    where
    {% if ignore_filter %}
        1 = 1
    {% elif ns.filter_mode == "changed_at" %}
        DBT_INTERNAL_WRAP_SRC.{{ ns.changed_at }} > (
            select
                max(DBT_INTERNAL_WRAP_TARG.{{ ns.changed_at }})
            from
                {{ ns.target_relation }} DBT_INTERNAL_WRAP_TARG
        )
    {% elif ns.filter_mode == "changed_at_per_scd_key" %}
        not exists (
            select
                null
            from
                {{ ns.target_relation }} DBT_INTERNAL_WRAP_TARG
            where
            {% for col in ns.scd_key_columns %}
                {% if not loop.first %} and {% endif %} decode(DBT_INTERNAL_WRAP_SRC.{{ col }}, DBT_INTERNAL_WRAP_TARG.{{ col }}, 1, 0) = 1
            {% endfor %}
                and DBT_INTERNAL_WRAP_TARG.{{ ns.changed_at }} > DBT_INTERNAL_WRAP_SRC.{{ ns.changed_at }}
        )
    {% elif ns.filter_mode == "scd_key" %}
        not exists (
            select null from {{ ns.target_relation }} DBT_INTERNAL_WRAP_TARG
            where
            {% for col in ns.scd_key_columns %}
                {% if not loop.first %} and {% endif %} decode(DBT_INTERNAL_WRAP_SRC.{{ col }}, DBT_INTERNAL_WRAP_TARG.{{ col }}, 1, 0) = 1
            {% endfor %}
        )
    {% endif %}
{% endmacro %}


{% macro SCD__process_schema_changes(ns) %}
{# --------------------------------------------------------------------
    input:
        ns, scd settings namespace
        note: ns.target_columns, source_columns and target_relation and source_relation must be set in advance
    returns:
        true if the target relation changed
        false otherwise (also if errors occured )
    description:
        Processes schema changes
#}

    {# list of columns to change #}
    {% set to_add = [] %}
    {% set to_expand = [] %}
    {% set to_morph = [] %}
    {% set to_remove = [] %}

    {# gather append, expand, morph changes #}
    {# appending, expanding, or morphing #}
    {% for col in ns.source_columns %}
        {% set other_col = ns.target_columns | selectattr("name", "equalto", col.name) | first | default(none) %}
        {% if not other_col %}
            {% do to_add.append(col) %}
        {% elif other_col.can_expand_to(col) %}
            {% do to_expand.append(col) %}
        {% elif other_col.dtype != col.dtype %}
            {% do to_morph.append(col) %} 
        {% endif %}
    {% endfor %}

    {# gather remove changes
        NOTE: the source select naturally will not include generated columns
        we should never remove generated columns (unless they are no longer generated)
    #}
    {% for col in ns.target_columns %}
        {% if col.name | lower in ns.generated_columns %}
            {% continue %}
        {% endif %}
        {% set other_col = ns.source_columns | selectattr("name", "equalto", col.name) | first | default(none) %}
        {% if not other_col %}
            {% do to_remove.append(col) %}
        {% endif %}
    {% endfor %}

    {# check for errors #}
    {% if to_add and not ns.append %}
        {% do ns.errors.update({"append": to_add | map(attribute="name") | list}) %}
    {% endif %}

    {% if to_expand and not ns.expand %}
        {% do ns.errors.update({"expand": to_expand | map(attribute="name") | list}) %}
    {% endif %}

    {% if to_morph and not ns.morph %}
        {% do ns.errors.update({"morph": to_morph | map(attribute="name")  | list}) %}
    {% endif %}

    {% if to_remove and not ns.remove %}
        {% do ns.errors.update({"remove": to_remove | map(attribute="name") | list}) %}
    {% endif %}

    {% set changed = false %}

    {% if ns.errors %}
        {% do return(changed) %}
    {% endif %}
    
    {% if to_add %}
        {% do alter_relation_add_remove_columns(ns.target_relation, add_columns=to_add, remove_columns=[]) %}
        {% set changed = true %}
    {% endif %}

    {# NOTE: the dbt sql adapter only considers string type as elligible for expansion
        and only if the target column has lower string size than the source column
        With all the different string types in Oracle this seems rather confused
        They also ignore numeric types entirely even though a subet of those can expand as well
        In any case, expansion is actually done by calling alter_column_type using the type of
        the source column i.e. target.column <- alter(target_relation, name, source_type)
    #}
    {% if to_expand %}
        {% for col in to_expand %}
            {% do alter_column_type(ns.target_relation, col.name, col.data_type) %}
        {% endfor %}
        {% set changed = true %}
    {% endif %}
    
    {% if to_morph %}
        {% for col in to_morph %}
            {% do alter_column_type(ns.target_relation, col.name, col.data_type) %}
        {% endfor %}
        {% set changed = true %}
    {% endif %}
    
    {% if to_remove %}
        {% do alter_relation_add_remove_columns(ns.target_relation, add_columns=[], remove_columns=to_remove) %}
        {% set changed = true %}
    {% endif %}
    
    {% do return(changed) %}

{% endmacro %}


{% macro SCD__validate_scd_target_rows(ns)  %}
{# --------------------------------------------------------------------
    input:
        ns, scd settings namespace
    returns:
        nothing
    description:
        checks scd data integrity, accumulating errors in ns.errors
        to be run after merge
#}

    {% set sql %}
        select count(*) from {{ ns.target_relation }}
        where {{ ns.valid_from }} > {{ ns.valid_to }}
    {% endset %}

    {% set result = (run_query(sql).columns[0].values()) %}
    {% if (result | length > 0) and result[0] > 0 %}
        {% do ns.errors.update({"reversed": result[0]}) %}
    {% endif %}

    {% set sql %}
        select count(*) from {{ ns.target_relation }} a
        where exists (
            select null from {{ ns.target_relation }} b
            where a.{{ ns.primary_key }} != b.{{ ns.primary_key }}
            -- siden vi bruker fom/til vil det være likhet= i hver ende, så vi må bruke <>
            and a.{{ ns.valid_from }} < b.{{ ns.valid_to }}
            and a.{{ ns.valid_to }} > b.{{ ns.valid_from }}
        {% for col in ns.scd_key_columns %}
            and decode(a.{{ col }}, b.{{ col }}, 1, 0) = 1
        {% endfor %}
        )
    {% endset %}

    {% set result = (run_query(sql).columns[0].values()) %}
    {% if (result | length > 0) and result[0] > 0 %}
        {% do ns.errors.update({"overlaps": result[0]}) %}
    {% endif %}

    {% set sql %}
        select count(*) from {{ ns.target_relation }}
        group by
        {% for col in ns.scd_key_columns %}
            {% if not loop.first %} , {% endif %}
            {{ col }}
        {% endfor %}
        having sum({{ ns.valid_flag }}) > 1
    {% endset %}

    {% set result = (run_query(sql).columns[0].values()) %}
    {% if (result | length > 0) and result[0] > 0 %}
        {% do ns.errors.update({"multi_valid": result[0]}) %}
    {% endif %}

    {% set sql %}
        select
            count(*)
        from
        (
            select
                decode
                (
                    lag(a.{{ ns.valid_to }}) over
                    (
                        partition by
                        {% for col in ns.scd_key_columns %}
                            {% if loop.first %}
                            a.{{ col }}
                            {% else %}
                            , a.{{ col }}
                            {% endif %}
                        {% endfor %}
                        order by
                            a.{{ ns.valid_from }}, a.{{ ns.valid_to }}
                    )
                    , a.{{ ns.valid_from }}, 0
                    , null, 0
                    , 1
                ) as jumped
            from
                {{ ns.target_relation }} a
            where
                -- ignorerer rader som egentlig aldri er gyldig / umulig å avgjøre gyldighet med
                -- fordi kilden har endret på data uten å gi nytt oppdatert tidspunkt
                a.{{ ns.valid_from }} != a.{{ ns.valid_to }}
        ) b
        where
            b.jumped = 1
    {% endset %}

    {% set result = (run_query(sql).columns[0].values()) %}
    {% if (result | length > 0) and result[0] > 0 %}
        {% do ns.errors.update({"gaps": result[0]}) %}
    {% endif %}

    {% if ns.scd_hash_columns %}
        {% set sql %}
            select
                count(*)
            from
            (
                select
                    decode
                    (
                        lag({{ dbt_dvh_macros.SCD__sha256_hash(ns.scd_hash_columns, "a") }}) over
                        (
                            partition by
                            {% for col in ns.scd_key_columns %}
                                {% if loop.first %}
                                a.{{col}}
                                {% else %}
                                , a.{{col}}
                                {% endif %}
                            {% endfor %}
                            order by
                                a.{{ ns.valid_from }}, a.{{ ns.valid_to }}
                        )
                        , {{ dbt_dvh_macros.SCD__sha256_hash(ns.scd_hash_columns, "a") }}, 1
                        , 0
                    ) as same_as_prev
                from
                    {{ ns.target_relation }} a
            ) b
            where
                same_as_prev = 1
        {% endset %}

        {% set result = (run_query(sql).columns[0].values()) %}
        {% if (result | length > 0) and result[0] > 0 %}
            {% do ns.errors.update({"repetitions": result[0]}) %}
        {% endif %}
    {% endif %}

{% endmacro %}


{% macro SCD__etl_date() %}
{# --------------------------------------------------------------------
    input: none
    returns: an sql expression for run started
    description:
        used to set loaded_at/updated_at when not present in model
#}
    to_date('{{ run_started_at.astimezone(modules.pytz.timezone("Europe/Oslo")).replace(microsecond=0,tzinfo=None).isoformat() }}', 'yyyy-mm-dd"T"hh24:mi:ss')
{% endmacro %}


{% macro SCD__window(part_by, order_by, dir, alias) %}
{# --------------------------------------------------------------------
    input:
        part_by: list of strings, column names, to partition by
        order_by: list of strings, column names, to order by
        dir: string, direction to order by, asc/desc/ascending/descending
        alias: string, a name for the relation the columns belong to
    returns:
        an sql over() window expression
    description:
        used by SCD type 1 and 2 to generate history correctly
#}
{{
    "over(partition by " ~ alias ~ "." ~ (part_by | join(", " ~ alias ~ ".")) 
    ~ " order by " ~ alias ~ "." ~ (order_by | join( " " ~ dir ~ ", " ~ alias ~ ".")) ~ " " ~ dir 
    ~ ")"
}}
{% endmacro %}


{%- macro SCD__sha256_hash(columns, alias) -%}
    rawtohex(standard_hash(
    {%- for column in columns -%}
        {%- if loop.first -%}
            {{alias}}.{{ column }}
        {%- else -%}
            || '¿' || {{alias}}.{{ column }}
        {%- endif -%}
    {%- endfor -%}
            , 'SHA256'))
{%- endmacro -%}



{# DEPRECATED AND REMOVED - please use SCD__window
{%- macro over_partition_window(scd_key_cols, scd_hash_cols, changed_at, direction, alias) -%}
over (partition by
{%- for col in scd_key_cols -%}
{%- if not loop.first -%} , {%- endif %} {{ alias }}.{{ col }}
{%- endfor %} order by {{ alias }}.{{ changed_at }} {{ direction }}
{%- for col in scd_hash_cols -%}
, {{ alias }}.{{ col }} {{ direction }}
{%- endfor -%} )
{%- endmacro -%}
 #}