{% macro BREDAGG__ephemeral_star(model_name, relation_alias, prefix='', except=[]) %}
  {%- set columns = [] -%}
  
  {%- if execute -%}
    {%- for node_name, node in graph.nodes.items() -%}
      {%- if node.name == model_name -%}
        {%- for col_name in node.columns.keys() -%}
          {%- if col_name not in except -%}
            {%- do columns.append(relation_alias ~ "." ~ col_name ~ " as " ~ prefix ~ col_name) -%}
          {%- endif -%}
        {%- endfor -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}

  {{ columns | join(',\n    ') }}
{% endmacro %}

