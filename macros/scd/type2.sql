{% macro SCD__get_type2_merge_sql(ns) %}

{% set orderby_columns = [ns.changed_at] + ns.scd_hash_columns %}

merge into
    {{ ns.target_relation }} DBT_INTERNAL_SCD2_TARGET
using (
    with DBT_INTERNAL_CTE_FINAL as (
        select
            DBT_INTERNAL_SOURCE.*
            , DBT_INTERNAL_TARGET_JOIN.{{ ns.primary_key }} as DBT_INTERNAL_EXISTING_PRIMARY_KEY
            {% if ns.scd_hash_columns %}
                , case when
                    {% for col in ns.scd_hash_columns %}
                        {% if not loop.first %} and {% endif %}
                        decode(
                            DBT_INTERNAL_SOURCE.{{ col }}
                            , lag(DBT_INTERNAL_SOURCE.{{ col }}, 1, DBT_INTERNAL_TARGET_JOIN.{{ col }})
                                {{ SCD__window(ns.scd_key_columns, orderby_columns, "asc", "DBT_INTERNAL_SOURCE") }}
                            , 1, 0
                        ) = 1
                    {% endfor %}
                    then 1 else 0 end as DBT_INTERNAL_IS_REPETITION
                , lag(0, 1, 1) {{ SCD__window(ns.scd_key_columns, orderby_columns, "asc", "DBT_INTERNAL_SOURCE") }}
                    as DBT_INTERNAL_IS_FIRST_ROW_FOR_SCD_KEY
            {% endif %}
        from
            {{ ns.source_relation }} DBT_INTERNAL_SOURCE
            left join {{ ns.target_relation }} DBT_INTERNAL_TARGET_JOIN
                {% for col in ns.scd_key_columns %}
                    {% if loop.first %} on {% else %} and {% endif %}
                    decode(DBT_INTERNAL_TARGET_JOIN.{{ col }}, DBT_INTERNAL_SOURCE.{{ col }}, 1, 0) = 1
                {% endfor %}
                    and DBT_INTERNAL_TARGET_JOIN.{{ ns.valid_flag }} = 1
    )
    select
    {% for col in ns.target_columns %}
        {% if not loop.first %} , {% endif %}
        {% if col == ns.primary_key and ns.generate_primary_key %}
            {{ ns.expression_primary_key }} as {{ ns.primary_key }}
        {% elif col == ns.valid_from %}
            -- kan ikke bruke DBT_INTERNAL_IS_FIRST_ROW_FOR_SCD_KEY her fordi den raden kan ha blitt droppet
            -- pga duplikat fjerning (WHERE klausul kjører først)
            -- fiks 18.04.26: kun bruk created_at hvis det ikke finnes rad i target
            case when DBT_INTERNAL_EXISTING_PRIMARY_KEY is null and lag(0, 1, 1)
            {{ SCD__window(ns.scd_key_columns, orderby_columns, "asc", "DBT_INTERNAL_CTE_FINAL") }}
            = 1 then coalesce(DBT_INTERNAL_CTE_FINAL.{{ ns.created_at }}, {{ ns.valid_from_default }})
            else {{ ns.changed_at }} end
            as {{ ns.valid_from }}
        {% elif col == ns.valid_to %}
            lead({{ ns.changed_at }}, 1, {{ ns.valid_to_default }})
            {{ SCD__window(ns.scd_key_columns, orderby_columns, "asc", "DBT_INTERNAL_CTE_FINAL") }}
            as {{ ns.valid_to }}
        {% elif col == ns.valid_flag %}
            lead(0, 1, 1)
            {{ SCD__window(ns.scd_key_columns, orderby_columns, "asc", "DBT_INTERNAL_CTE_FINAL") }}
            as {{ ns.valid_flag }}
        {% elif col == ns.updated_at and ns.generate_updated_at %}
            {{ SCD__etl_date() }} as {{ ns.updated_at }}
        {% elif col == ns.loaded_at and ns.generate_loaded_at %}
            {{ SCD__etl_date() }} as {{ ns.loaded_at }}
        {% else %}
            DBT_INTERNAL_CTE_FINAL.{{ col }}
        {% endif %}
    {% endfor %}
    from
        DBT_INTERNAL_CTE_FINAL
    {% if ns.scd_hash_columns %}
    where
        DBT_INTERNAL_CTE_FINAL.DBT_INTERNAL_IS_REPETITION = 0
        or (
            -- fixes case where source NULL is compared with NULL because no target row exists
            -- note that if target exists and is NULL then it NULL=NULL is correct to drop
            DBT_INTERNAL_CTE_FINAL.DBT_INTERNAL_IS_FIRST_ROW_FOR_SCD_KEY = 1
            and DBT_INTERNAL_CTE_FINAL.DBT_INTERNAL_EXISTING_PRIMARY_KEY is null
        )
    {% endif %}
    union all select
        {% for col in ns.target_columns %}
            {% if not loop.first %} , {% endif %}
            {% if col == ns.primary_key %}
                DBT_INTERNAL_CTE_FINAL.DBT_INTERNAL_EXISTING_PRIMARY_KEY as {{ ns.primary_key }}
            {% elif col == ns.updated_at %}
                {% if ns.generate_updated_at %}
                    {{ SCD__etl_date() }} as {{ ns.updated_at }}
                {% else %}
                    min(DBT_INTERNAL_CTE_FINAL.{{ ns.updated_at }}) as {{ ns.updated_at }}
                {% endif %}
            {% elif col == ns.valid_to %}
                min(DBT_INTERNAL_CTE_FINAL.{{ ns.changed_at }}) as {{ ns.valid_to }}
            {% else %}
                null as {{ col }}
            {% endif %}
        {% endfor %}
    from
        DBT_INTERNAL_CTE_FINAL
    where
        DBT_INTERNAL_CTE_FINAL.DBT_INTERNAL_EXISTING_PRIMARY_KEY is not null
    {% if ns.scd_hash_columns %}
        and DBT_INTERNAL_CTE_FINAL.DBT_INTERNAL_IS_REPETITION = 0
    {% endif %}
    group by
        DBT_INTERNAL_CTE_FINAL.DBT_INTERNAL_EXISTING_PRIMARY_KEY
) DBT_INTERNAL_SCD2_SOURCE

on (
    DBT_INTERNAL_SCD2_TARGET.{{ ns.primary_key }} = DBT_INTERNAL_SCD2_SOURCE.{{ ns.primary_key}}
)

when matched then
    update set
        DBT_INTERNAL_SCD2_TARGET.{{ ns.valid_flag }} = 0
        , DBT_INTERNAL_SCD2_TARGET.{{ ns.valid_to }} = DBT_INTERNAL_SCD2_SOURCE.{{ ns.valid_to }}
        , DBT_INTERNAL_SCD2_TARGET.{{ ns.updated_at }} = DBT_INTERNAL_SCD2_SOURCE.{{ ns.updated_at }}

when not matched then
    insert (
        {% for col in ns.target_columns %}
            {% if not loop.first %} , {% endif %}
            DBT_INTERNAL_SCD2_TARGET.{{ col }}
        {% endfor %}
    )
    values (
        {% for col in ns.target_columns %}
            {% if not loop.first %} , {% endif %}
            DBT_INTERNAL_SCD2_SOURCE.{{ col }}
        {% endfor %}
    )
{% endmacro %}
