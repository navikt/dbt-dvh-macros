
{% materialization scd, adapter="oracle", supported_languages=["sql"] %}
{#------------------------------------------------------------------------------------------------#}

    {# read scd config and perform basic validation of contents, use namespace to avoid pollution #}
    {% set ns = dbt_dvh_macros.SCD__validate_config() %}
    {% if ns.errors %}
        {% do exceptions.raise_compiler_error("invalid scd configuration for " ~ this ~ ": " ~ ns.errors) %}
    {% endif %}

    {% set language = "sql" %}

    {# define relations and update namespace with the relations #}
    {% set existing_relation = load_relation(this) %}
    {% if existing_relation and not existing_relation.is_table %}
        {% do exceptions.raise_compiler_error("existing relation " ~ existing_relation ~ " is not a table!")%}
    {% endif %}
    {% set ns.target_relation = this.incorporate(type="table") %}
    {% set ns.source_relation = make_temp_relation(ns.target_relation) %}
    {% set to_drop = [ns.source_relation] %}

    {# pre-hooks: interestingly, dbt core adapters do full refresh before this #}
    {{ run_hooks(pre_hooks, inside_transaction=false) }}
    {{ run_hooks(pre_hooks, inside_transaction=true) }}

    {# create the source table EMPTY here, and insert the rows further below.

        The split is not about datatype precision: a plain "create table as <sql>" would give us exactly
        the same database-derived datatypes, since the "where 1 = 0" only removes rows, not type info.
        It exists because the steps are circular and this is the only order that resolves them:
            1. we need precise source datatypes, which only a real table gives us
                (get_column_schema_from_query is not precise enough, hence materializing at all)
                needed by SCD__process_schema_changes for can_expand_to/dtype (expand and morph)
                and by ns.changed_at_data_type, which types valid_from/valid_to
            2. when the target does not exist we derive it FROM this table (see make_target_relation)
            3. the row filter references the target (max(changed_at), not exists), so the target must
                already exist before we can compute which rows to insert
        So: empty source table -> target -> filtered insert. A single filtered CTAS is impossible on a
        first run, because step 3 needs what step 2 builds out of step 1.

        Bonus: the empty create is cheap, so config and schema change errors below are raised before we
        load any data at all.
        TODO: ask dbt-oracle adapter devs to fix get_column_schema_from_query so precise datatypes are given
    #}
    {% call statement("create_source_table", language=language, fetch_result=false) %}
        {{ create_table_as(true, ns.source_relation, get_empty_subquery_sql(sql), language) }}
    {% endcall %}
    {% set ns.source_columns = get_columns_in_relation(ns.source_relation) %}
    {% do dbt_dvh_macros.SCD__validate_source_columns_against_config(ns) %}
    {% if ns.errors %}
        {% do adapter.drop_relation(ns.source_relation) %} {# cleanup temp table #}
        {% do exceptions.raise_compiler_error("the model select for " ~ ns.target_relation ~ " does not match the scd configuration: " ~ ns.errors) %}
    {% endif %}

    {# handle full refresh #}
    {% set full_refresh_mode = should_full_refresh() %}
    {% if existing_relation and full_refresh_mode %}
        {% set backup_relation = existing_relation.incorporate(
            path={"identifier": existing_relation.identifier ~ "__dbt_backup"}
        ) %}
        {# not clear why oracle insists on using adapter.* here #}
        {% do adapter.drop_relation(backup_relation) %}  {# no-op if it doesnt exist #}
        {% do adapter.rename_relation(existing_relation, backup_relation) %}
        {% do to_drop.append(backup_relation) %}
        {% set existing_relation = none %}
    {% endif %}

    {# handle schema changes except if target doesnt exist or full refresh renamed it #}
    {% if existing_relation %}

        {% set ns.target_columns = get_columns_in_relation(ns.target_relation) %}
        {% set changed = dbt_dvh_macros.SCD__process_schema_changes(ns) %}
        {% if ns.errors %}
            {% do adapter.drop_relation(ns.source_relation) %} {# cleanup temp table #}
            {% do exceptions.raise_compiler_error("schema changes required by " ~ ns.target_relation ~ " are not enabled in schema_changes_enabled: " ~ ns.errors) %}
        {% elif changed %} {# update columns #}
            {% set ns.target_columns = adapter.get_columns_in_relation(ns.target_relation) %}
        {% endif %}
    
    {# ...or create the target #}
    {% else %}

        {# create empty target table from the source table, ie the target inherits the source datatypes
            this is step 2 of the ordering described at create_source_table, and the reason the source
            table has to exist (empty) before the target does #}
        {% call statement("make_target_relation", fetch_result=false, language=language) %}
            {{ create_table_as(false, ns.target_relation, get_empty_subquery_sql("select * from " ~ ns.source_relation), language) }}
        {% endcall %}

        {# Ordinarily column level changes are handled by macros/adapter using
            adapter.check_and_quote_identifier(column.name, model.columns)
            and either column.data_type or column.expanded_data_type
            The Oracle Adapter does not implement the expanded_data_type property/method
            so it defaults to the SQL adapters version, which just returns column.data_type

            In any case, doing it this way without checking or quoting might cause trouble
            or it might not. We have already tried to compare source and target columns without
            checking and quoting, so this would have to be changed everywhere in the scd settings validation. #}
        {% if ns.generate_primary_key %}
            {% call statement("add_primary_key_to_target_relation", language=language) %}
                alter table {{ ns.target_relation }} add {{ ns.primary_key }} number(38, 0) primary key
            {% endcall %}
        {% endif %}

        {% call statement("add_valid_columns_target_relation", language=language) %}
            alter table {{ ns.target_relation }}
                add {{ ns.valid_from }} {{ns.changed_at_data_type}} not null
                add {{ ns.valid_to }} {{ns.changed_at_data_type}} not null
                add {{ ns.valid_flag }} number(1, 0) not null
        {% endcall %}
        
        {% if ns.generate_updated_at or ns.generate_loaded_at %}
            {% call statement("add_etl_date_columns_to_target_relation", language=language) %}
                alter table {{ ns.target_relation }}
                {% if ns.generate_updated_at %}
                    add {{ ns.updated_at }} date not null
                {% endif %}
                {% if ns.generate_loaded_at %}
                    add {{ ns.loaded_at }} date not null
                {% endif %}
            {% endcall %}
        {% endif %}

        {# set finalized target columns 
            NOTE: we could in theory do this without asking the database
            consider this if performance is an issue #}
        {% set ns.target_columns = adapter.get_columns_in_relation(ns.target_relation) %}
    {% endif %}


    {% set insert_sql = dbt_dvh_macros.SCD__get_scd_model_source_insert_sql(ns, not existing_relation, sql) %}
    {# now that the target exists we can compute the filter and load the rows, ie step 3 of the ordering
        described at create_source_table. ignore_filter is passed when the target was just created,
        since there is then nothing to filter against.

        Jinja cannot catch a database error, so the statement is wrapped in a plsql block that drops the
        temp table in its exception handler and re-raises. Without this a failure here leaks the temp
        table, and since make_temp_relation appends a timestamp to the name, every failed run leaks a new
        one. Requires dbt-oracle >= 1.11.1, which is where this adapter macro was introduced. #}
    {% call statement("insert_to_source_table", language=language, fetch_result=false) %}
        {{ oracle__wrap_incremental_sql_with_tmp_cleanup(insert_sql, ns.source_relation) }}
    {% endcall %}

    {# datatypes no longer needed, change to pure column names for the sql scripts #}
    {% set ns.source_columns = ns.source_columns | map(attribute="name") | map("lower") | list %}
    {% set ns.target_columns = ns.target_columns | map(attribute="name") | map("lower") | list %}


    {% if ns.scd_type == 0 %}
        {% set merge_sql = dbt_dvh_macros.SCD__get_type0_merge_sql(ns) %}
    {% elif ns.scd_type == 1 %}
        {% set merge_sql = dbt_dvh_macros.SCD__get_type1_merge_sql(ns) %}
    {% elif ns.scd_type == 2 %}
        {% set merge_sql = dbt_dvh_macros.SCD__get_type2_merge_sql(ns) %}
    {% endif %}
    
    {# wrapped for temp table cleanup on failure, see insert_to_source_table above #}
    {% call statement("main", language=language) %}
        {{ oracle__wrap_incremental_sql_with_tmp_cleanup(merge_sql, ns.source_relation) }}
    {% endcall %}

    {% do dbt_dvh_macros.SCD__validate_scd_target_rows(ns) %}
    {# NB: do NOT drop the source table before raising here, even though it leaks it.

        At this point the merge has succeeded but is not committed, and raising lets dbt roll it back.
        truncate and drop are DDL, which implicitly commits, so cleaning up here would commit the very
        merge these checks just rejected. It would not even work: the source table has rows now, so the
        drop hits ORA-14452, which oracle__get_drop_sql silently swallows. Rolling back first is not an
        option either, since dbt exposes no rollback to jinja. Leaking a temp table beats persisting
        data we have proven invalid.
        The early error paths above can drop safely because only DDL precedes them, so nothing is pending.
        TODO: handle errors better #}
    {% if ns.errors %}
        {% do exceptions.raise_database_error("scd integrity checks failed for " ~ ns.target_relation ~ ": " ~ ns.errors) %}
    {% endif %}

    {# model variable, like sql variable, is implicit, created from parsing stage #}
    {% do persist_docs(ns.target_relation, model) %}

    {{ run_hooks(post_hooks, inside_transaction=true) }}
    
    {# COMMIT #}
    {% do adapter.commit() %}
    
    {# the truncate is load bearing, not redundant: the source table is a global temporary table created
        "on commit preserve rows", so its rows survive the commit above and a bare drop would fail with
        ORA-14452, which oracle__get_drop_sql swallows silently, leaving the table behind #}
    {% for rel in to_drop %}
        {% do adapter.truncate_relation(rel) %}
        {% do adapter.drop_relation(rel) %}
    {% endfor %}

    {{ run_hooks(post_hooks, inside_transaction=false) }}

    {# note: is_table is always true if relation exists, otherwise the access results in None #}
    {% set should_revoke = should_revoke(existing_relation.is_table, full_refresh_mode) %}
    {% set grant_config = config.get("grants") %}
    {% do apply_grants(ns.target_relation, grant_config, should_revoke=should_revoke) %}

    {{ return({'relations': [ns.target_relation]}) }}

{% endmaterialization %}
