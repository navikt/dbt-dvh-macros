{# macros/sync_multi_source_comments.sql #}
{% macro BREDAGG__sync_multi_source_comments(source_list) %}
    
    {% if execute %}
        {# Loop through every source provided in the list #}
        {% for src in source_list %}
            
            {% set source_name = src[0] %}
            {% set source_table = src[1] %}
            
            {% set source_node = source(source_name, source_table) %}
            {% set src_owner = source_node.schema | upper %}
            {% set src_table = source_node.identifier | upper %}
            
            {# Fetch comments for this specific source #}
            {% set find_comments_query %}
                SELECT column_name, comments 
                FROM all_col_comments 
                WHERE owner = '{{ src_owner }}' 
                  AND table_name = '{{ src_table }}'
                  AND comments IS NOT NULL
            {% endset %}
            
            {% set comment_rows = run_query(find_comments_query) %}
            
            {# Apply comments to the new table if the columns match #}
            {% if comment_rows %}
                {% for row in comment_rows %}
                    {% set col_name = row['COLUMN_NAME'] %}
                    {% set comment_text = row['COMMENTS'] | replace("'", "''") %}
                    
                    {# Oracle will safely fail or skip if the column doesn't exist on {{ this }} #}
                    {# To avoid query syntax failures on non-existent columns, we catch errors safely #}
                    {% set apply_comment_sql %}
                        BEGIN
                            EXECUTE IMMEDIATE 'COMMENT ON COLUMN {{ this }}."{{ col_name }}" IS ''{{ comment_text }}''';
                        EXCEPTION
                            WHEN OTHERS THEN
                                NULL; -- Safely skip if the column doesn't exist in the new dbt table
                        END;
                    {% endset %}
                    
                    {% do run_query(apply_comment_sql) %}
                {% endfor %}
            {% endif %}
            
        {% endfor %}
    {% endif %}

{% endmacro %}
