{# Drop transient relations left over from the run: the temporary source relation and, when the
    target was rebuilt from scratch, the backup of the previous target.
    The truncate is load bearing, not redundant: the source relation is a global temporary table
    created "on commit preserve rows", so its rows survive a commit and a bare drop would fail with
    ORA-14452, which oracle__get_drop_sql swallows silently, leaving the table behind. #}
{% macro SCD__drop_relations(relations) %}
    {% for relation in relations %}
        {% do adapter.truncate_relation(relation) %}
        {% do adapter.drop_relation(relation) %}
    {% endfor %}
{% endmacro %}


{# Move a relation out of the way so its name is free for a new target table.
    A table is renamed, which is cheap and keeps the data either way. Oracle can rename neither a
    view nor a materialized view, so those are copied into a table when preserve_data is set, and
    dropped outright otherwise.
    input:
        relation: the relation to move
        preserve_data: whether the data of a view or materialized view is worth copying
    returns:
        the backup relation, or none when the relation was dropped without being kept #}
{% macro SCD__move_relation_to_backup(relation, preserve_data=false) %}

    {# the backup only ever holds a table, and the type has to be right before the drop below,
        since oracle__get_drop_sql branches on it and would silently swallow the ORA-12003 raised
        by dropping a leftover backup table as if it were a materialized view #}
    {% set backup_relation = relation.incorporate(
        type="table", path={"identifier": relation.identifier ~ "__dbt_backup"}
    ) %}

    {# make sure a backup left behind by a crashed run does not block the rename/copy below #}
    {% do adapter.drop_relation(backup_relation) %}

    {% if relation.is_table %}
        {% do adapter.rename_relation(relation, backup_relation) %}
    {% elif preserve_data %}
        {% call statement("copy_relation_to_backup", language="sql", fetch_result=false) %}
            {{ create_table_as(false, backup_relation, "select * from " ~ relation, language="sql") }}
        {% endcall %}
        {% do adapter.drop_relation(relation) %}
    {% else %}
        {% do adapter.drop_relation(relation) %}
        {% do return(none) %}
    {% endif %}

    {% do return(backup_relation) %}

{% endmacro %}


{# Add the columns the scd materialization maintains itself to a freshly created target table.
    Oracle rejects "add <column> not null" without a default on a table that holds rows with
    ORA-01758, so this has to run while the table is still empty. #}
{% macro SCD__add_scd_columns(ns, relation) %}

    {% if ns.generate_primary_key %}
        {% call statement("add_primary_key_to_target_relation", language="sql") %}
            alter table {{ relation }} add {{ ns.primary_key }} number(38, 0) primary key
        {% endcall %}
    {% endif %}

    {% call statement("add_valid_columns_target_relation", language="sql") %}
        alter table {{ relation }}
            add {{ ns.valid_from }} {{ ns.changed_at_data_type }} not null
            add {{ ns.valid_to }} {{ ns.changed_at_data_type }} not null
            add {{ ns.valid_flag }} number(1, 0) not null
    {% endcall %}

    {% if ns.generate_updated_at or ns.generate_loaded_at %}
        {% call statement("add_etl_date_columns_to_target_relation", language="sql") %}
            alter table {{ relation }}
            {% if ns.generate_updated_at %}
                add {{ ns.updated_at }} date not null
            {% endif %}
            {% if ns.generate_loaded_at %}
                add {{ ns.loaded_at }} date not null
            {% endif %}
        {% endcall %}
    {% endif %}

{% endmacro %}


{% materialization scd, adapter="oracle", supported_languages=["sql"] %}

    {# 1. Read and validate user config #}
    {% set ns = dbt_dvh_macros.SCD__validate_config() %}
    {% if ns.errors %}
        {% do exceptions.raise_compiler_error("invalid scd configuration for " ~ this ~ ": " ~ ns.errors) %}
    {% endif %}

    {# 2. Run pre-hooks
        These have to run before the model select is used below, since a pre-hook may create or
        refresh an object the select depends on.
        Note: the DDL that follows implicitly commits, and with it the transaction opened by the
            in-transaction pre-hooks. Oracle's own incremental materialization has the same quirk.
    #}
    {{ run_hooks(pre_hooks, inside_transaction=false) }}
    {{ run_hooks(pre_hooks, inside_transaction=true) }}

    {# 3. Create empty source relation to store incoming data, infer datatypes,
        and validate query against config.
        TODO: ask dbt-oracle adapter devs to include precise datatypes in get_column_schema_from_query
    #}
    {% set ns.source_relation = make_temp_relation(this.incorporate(type="table")) %}
    {% set ns.target_relation = this.incorporate(type="table") %}
    {% set to_drop = [ns.source_relation] %}
    {% call statement("create_empty_source_table", language="sql", fetch_result=false) %}
        {{ create_table_as(true, ns.source_relation, get_empty_subquery_sql(sql), language="sql") }}
    {% endcall %}
    {% set ns.source_columns = adapter.get_columns_in_relation(ns.source_relation) %}
    {% do dbt_dvh_macros.SCD__validate_source_columns_against_config(ns) %}
    {% if ns.errors %}
        {% do dbt_dvh_macros.SCD__drop_relations(to_drop) %}
        {% do exceptions.raise_compiler_error("the model select for " ~ ns.target_relation ~ " does not match the scd configuration: " ~ ns.errors) %}
    {% endif %}

    {# 4. Check existing relation #}
    {% set existing_relation = load_relation(this) %}
    
    {% set full_refresh_mode = should_full_refresh() %}

    {% if existing_relation is not none %}

        {% if full_refresh_mode %}

            {# Get the existing relation out of the way and pretend it doesnt exist
                Note: Old backup is removed, so data is destroyed.
                    This is exactly what oracle incremental does per v1.11.1.
                The backup only guards against a crash during this run and is dropped again at the
                end of it, so a view or materialized view is not worth copying.
            #}
            {# NB! From here to_drop holds the only copy of the previous data. Do not raise below
                without dropping this append first, since the raise paths drop everything in
                to_drop and would destroy exactly what the user needs to recover. #}
            {% set backup_relation = dbt_dvh_macros.SCD__move_relation_to_backup(existing_relation) %}
            {% if backup_relation is not none %}
                {% do to_drop.append(backup_relation) %}
            {% endif %}

            {% set existing_relation = none %}

            {# ...At this point we would like to create the target table, but that requires more knowledge about
                the column datatypes of the model query... #}

        {% else %}

            {# Demand required columns are present #}
            {% do dbt_dvh_macros.SCD__validate_existing_relation_against_config(ns, existing_relation) %}
            {% if ns.errors %}
                {% do dbt_dvh_macros.SCD__drop_relations(to_drop) %}
                {% do exceptions.raise_compiler_error(
                    "existing relation " ~ existing_relation ~ " cannot be used as an scd target: " ~ ns.errors
                    ~ ". Either it was not built by the scd materialization, or the scd configuration"
                    ~ " has been changed to use different column names. Drop it or rename the columns"
                    ~ " to match the configuration."
                ) %}
            {% endif %}

            {# Force table type in case user migrated from a view or materialized view
                Note: Old backup is removed, so data is destroyed.
                    As with full-refresh, a crash may require manual cleanup.
                    Also, no to_drop append here, the backup is transient as a backup,
                    since it becomes the new a target table.
            #}
            {% if not existing_relation.is_table %}
                {% set backup_relation = dbt_dvh_macros.SCD__move_relation_to_backup(existing_relation, preserve_data=true) %}
                {% do adapter.rename_relation(backup_relation, existing_relation) %}
                {% set existing_relation = existing_relation.incorporate(type="table") %}
            {% endif %}

        {% endif %}

    {% endif %}

    {# 5.A Handle schema changes (this path is not taken in full refresh) #}
    {% if existing_relation is not none %}
        {% set ns.target_columns = adapter.get_columns_in_relation(ns.target_relation) %}
        {% set changed = dbt_dvh_macros.SCD__process_schema_changes(ns) %}
        {% if ns.errors %}
            {% do dbt_dvh_macros.SCD__drop_relations(to_drop) %}
            {% do exceptions.raise_compiler_error("schema changes required by " ~ ns.target_relation ~ " are not enabled in schema_changes_enabled: " ~ ns.errors) %}
        {% elif changed %} {# update columns #}
            {% set ns.target_columns = adapter.get_columns_in_relation(ns.target_relation) %}
        {% endif %}

    {# 5.B Create a target table based on source table with added SCD columns #}
    {% else %}
        {% call statement("make_target_table", fetch_result=false, language="sql") %}
            {{ create_table_as(false, ns.target_relation, get_empty_subquery_sql("select * from " ~ ns.source_relation), language="sql") }}
        {% endcall %}

        {% do dbt_dvh_macros.SCD__add_scd_columns(ns, ns.target_relation) %}

        {% set ns.target_columns = adapter.get_columns_in_relation(ns.target_relation) %}
    {% endif %}

    {# 6. Insert the model select into the source relation #}
    {% set insert_sql = dbt_dvh_macros.SCD__get_scd_model_source_insert_sql(ns, existing_relation is none, sql) %}
    {% call statement("insert_to_source_table", language="sql", fetch_result=false) %}
        {{ oracle__wrap_incremental_sql_with_tmp_cleanup(insert_sql, ns.source_relation) }}
    {% endcall %}

    {# 7. Run Merge ETL
        note: datatypes no longer needed, change to pure column names for the sql scripts 
        TODO: this a bit of a code smell
    #}
    {% set ns.source_columns = ns.source_columns | map(attribute="name") | map("lower") | list %}
    {% set ns.target_columns = ns.target_columns | map(attribute="name") | map("lower") | list %}

    {% if ns.scd_type == 0 %}
        {% set merge_sql = dbt_dvh_macros.SCD__get_type0_merge_sql(ns) %}
    {% elif ns.scd_type == 1 %}
        {% set merge_sql = dbt_dvh_macros.SCD__get_type1_merge_sql(ns) %}
    {% elif ns.scd_type == 2 %}
        {% set merge_sql = dbt_dvh_macros.SCD__get_type2_merge_sql(ns) %}
    {% endif %}
    
    {% call statement("main", language="sql") %}
        {{ oracle__wrap_incremental_sql_with_tmp_cleanup(merge_sql, ns.source_relation) }}
    {% endcall %}

    {# 8. Validate result of ETL
        NB! No cleanup before this raise, deliberately. Dropping is DDL and implicitly commits, which
        would commit the very merge these checks just rejected. Leaving to_drop alone also keeps the
        full refresh backup alive, so the previous data is still recoverable.
    #}
    {% do dbt_dvh_macros.SCD__validate_scd_target_rows(ns) %}
    {% if ns.errors %}
        {% do exceptions.raise_database_error("scd integrity checks failed for " ~ ns.target_relation ~ ": " ~ ns.errors) %}
    {% endif %}

    {# 9. Run post-hooks before committing ETL #}
    {{ run_hooks(post_hooks, inside_transaction=true) }}

    {# 10. Commit ETL #}
    {% do adapter.commit() %}

    {# 11. Do post cleanup and handle docs and grants #}
    {% do dbt_dvh_macros.SCD__drop_relations(to_drop) %}
    
    {{ run_hooks(post_hooks, inside_transaction=false) }}

    {% do persist_docs(ns.target_relation, model) %}
    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
    {% set grant_config = config.get("grants") %}
    {% do apply_grants(ns.target_relation, grant_config, should_revoke=should_revoke) %}

    {{ return({'relations': [ns.target_relation]}) }}

{% endmaterialization %}