{% macro SCD__get_type1_merge_sql(ns) %}

{% set orderby_columns = [ns.changed_at] + ns.scd_hash_columns %}

merge into
    {{ ns.target_relation }} DBT_INTERNAL_SCD1_TARGET
using (
    with DBT_INTERNAL_CTE_FINAL as (
        select
            DBT_INTERNAL_SOURCE.*
            , case when
                row_number() {{ dbt_dvh_macros.SCD__window(ns.scd_key_columns, orderby_columns, "desc", "DBT_INTERNAL_SOURCE") }}
                = 1 then 1 else 0 end
                as DBT_INTERNAL_IS_LAST_ROW_FOR_SCD_KEY
            , DBT_INTERNAL_TARGET_JOIN.{{ ns.primary_key }} as DBT_INTERNAL_EXISTING_PRIMARY_KEY
            {% if ns.scd_hash_columns %}
            , case when DBT_INTERNAL_TARGET_JOIN.{{ ns.primary_key }} is not null
                {% for col in ns.scd_hash_columns %}
                    and decode(DBT_INTERNAL_SOURCE.{{ col }}, DBT_INTERNAL_TARGET_JOIN.{{ col }}, 1, 0) = 1
                {% endfor %}
                then 1 else 0 end
                as DBT_INTERNAL_IS_REPETITION
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
            coalesce(
                DBT_INTERNAL_CTE_FINAL.DBT_INTERNAL_EXISTING_PRIMARY_KEY
                , {{ ns.expression_primary_key }}
            ) as {{ ns.primary_key }}
        {% elif col == ns.valid_from %}
            coalesce(
                DBT_INTERNAL_CTE_FINAL.{{ ns.created_at }}
                , {{ ns.valid_from_default }}
            ) as {{ ns.valid_from }}
        {% elif col == ns.valid_to %}
            {{ ns.valid_to_default }} as {{ ns.valid_to }}
        {% elif col == ns.valid_flag %}
            1 as {{ ns.valid_flag }}
        {% elif col == ns.updated_at and ns.generate_updated_at %}
            {{ dbt_dvh_macros.SCD__etl_date() }} as {{ ns.updated_at }}
        {% elif col == ns.loaded_at and ns.generate_loaded_at %}
            {{ dbt_dvh_macros.SCD__etl_date() }} as {{ ns.loaded_at }}
        {% else %}
            DBT_INTERNAL_CTE_FINAL.{{ col }}
        {% endif %}
    {% endfor %}
    from
        DBT_INTERNAL_CTE_FINAL
    where
        DBT_INTERNAL_CTE_FINAL.DBT_INTERNAL_IS_LAST_ROW_FOR_SCD_KEY = 1
    {% if ns.scd_hash_columns %}
        and DBT_INTERNAL_CTE_FINAL.DBT_INTERNAL_IS_REPETITION = 0
    {% endif %}
) DBT_INTERNAL_SCD1_SOURCE

on (
    DBT_INTERNAL_SCD1_TARGET.{{ ns.primary_key }} = DBT_INTERNAL_SCD1_SOURCE.{{ ns.primary_key}}
)

when matched then
    update set
        {% for col in ns.data_columns %}
            {% if not loop.first %} , {% endif %}
            DBT_INTERNAL_SCD1_TARGET.{{ col }} = DBT_INTERNAL_SCD1_SOURCE.{{ col }}
        {% endfor %}
    /*where
        DBT_INTERNAL_SCD1_TARGET.{{ ns.changed_at }} < DBT_INTERNAL_SCD1_SOURCE.{{ ns.changed_at }}*/

when not matched then
    insert (
        {% for col in ns.target_columns %}
            {% if not loop.first %} , {% endif %}
            DBT_INTERNAL_SCD1_TARGET.{{ col }}
        {% endfor %}
    )
    values (
        {% for col in ns.target_columns %}
            {% if not loop.first %} , {% endif %}
            DBT_INTERNAL_SCD1_SOURCE.{{ col }}
        {% endfor %}
    )

{% endmacro %}
