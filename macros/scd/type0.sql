{% macro SCD__get_type0_merge_sql(ns) %}

{% set orderby_columns = [ns.changed_at] + ns.scd_hash_columns %}

merge into
    {{ ns.target_relation }} DBT_INTERNAL_SCD0_TARGET
using (
    with DBT_INTERNAL_CTE_FINAL as (
        select
            DBT_INTERNAL_SOURCE.*
            , case when
                row_number() {{ SCD__window(ns.scd_key_columns, orderby_columns, "desc", "DBT_INTERNAL_SOURCE") }}
                = 1 then 1 else 0 end
                as DBT_INTERNAL_IS_LAST_ROW_FOR_SCD_KEY
        from
            {{ ns.source_relation }} DBT_INTERNAL_SOURCE
    )
    select
        {% for col in ns.target_columns %}
            {% if not loop.first %} , {% endif %}
            {% if col == ns.primary_key and ns.generate_primary_key %}
                {{ ns.expression_primary_key }} as {{ ns.primary_key }}
            {% elif col == ns.valid_from %}
                coalesce(DBT_INTERNAL_CTE_FINAL.{{ ns.created_at }}, {{ ns.valid_from_default }}) as {{ ns.valid_from }}
            {% elif col == ns.valid_to %}
                {{ ns.valid_to_default }} as {{ ns.valid_to }}
            {% elif col == ns.valid_flag %}
                1 as {{ ns.valid_flag }}
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
    where
        DBT_INTERNAL_CTE_FINAL.DBT_INTERNAL_IS_LAST_ROW_FOR_SCD_KEY = 1
) DBT_INTERNAL_SCD0_SOURCE

on (
{% for col in ns.scd_key_columns %}
    {% if not loop.first %} and {% endif %}
    DBT_INTERNAL_SCD0_TARGET.{{ col }} = DBT_INTERNAL_SCD0_SOURCE.{{ col }}
{% endfor %}
)

when not matched then
    insert (
        {% for col in ns.target_columns %}
            {% if not loop.first %} , {% endif %}
            DBT_INTERNAL_SCD0_TARGET.{{ col }}
        {% endfor %}
    )
    values (
        {% for col in ns.target_columns %}
            {% if not loop.first %} , {% endif %}
            DBT_INTERNAL_SCD0_SOURCE.{{ col }}
        {% endfor %}
    )
{% endmacro %}
