{%- macro audit_multiple_tables(envs=[], tables=[], metrics=[], dimensions=[], dimensions_count=[], filters=[]) -%}

{%- set table_name_combinations = [] -%}
{%- set tables_list = [] -%}
{%- set table_relation_info = [] -%}
{%- set include_cols = [] -%}
{%- set metrics_env = [] -%}
{%- set dimensions_env = [] -%}
{% set group_length = 1 + dimensions|length %}

{%- for env in envs -%}
    {%- for table in tables -%}
        {%- do table_name_combinations.append(( target.dbname, env, table)) -%}
    {%- endfor -%}
{%- endfor -%}

{%- for table_info in table_name_combinations -%}
    {%- set relation_exists = adapter.get_relation(
            database=target.dbname,
            schema=table_info[1],
            identifier=table_info[2]
        )
    -%}
    {%- if relation_exists -%}
        {%- set relation = api.Relation.create(database=target.dbname, schema=table_info[1], identifier=table_info[2]) -%}
        {%- do tables_list.append(relation) -%}
        {%- do table_relation_info.append(relation.database ~ '.' ~relation.schema ~ '.' ~ relation.identifier) -%}
    {%- endif -%}
{%- endfor -%}

{%- set dimension_selection -%}
    {%- for column_name in dimensions -%} 
        {{ column_name }},
    {%- endfor -%}
{%- endset -%}

{%- set alias_selection -%}
    {%- for column_name in dimensions -%}
        {%- if 'as' in column_name -%}
            d.{{ column_name.split(' as ')[1] }},
        {% else %}
            d.{{ column_name }},
        {%- endif -%}
    {%- endfor -%}
{%- endset -%}

{%- set dimension_counts -%}
    {% for dim in dimensions_count %}
        {% for env in envs %}
            {{ env }}_{{ dim }}_distinct_count,
            {%- do dimensions_env.append( env ~ '_' ~ dim ~ '_distinct_count' ) -%}
        {% endfor %}
        {% for env in envs %}
            {{ env }}_{{ dim }}_null_count,
            {%- do dimensions_env.append( env ~ '_' ~ dim ~ '_null_count' ) -%}
        {% endfor %}
    {% endfor %}
{%- endset -%}

{%- set column_selection -%}
    {%- for column_name in metrics -%}
        SUM({{ column_name }}) AS sum_{{ column_name }}
        {%- if not loop.last -%}
        ,
        {%- endif -%}
    {%- endfor -%}
{%- endset -%}

{%- for column_name in metrics -%}
    {%- for table_relation in table_relation_info -%}
        {%- do metrics_env.append(table_relation.split('.')[1] ~ '_' ~ column_name) -%}
    {%- endfor -%}
{%- endfor -%}

WITH data AS (
    SELECT 
        *,
        REPLACE(_dbt_source_relation, '"', '') AS _dbt_source_relation_no_quotes
    FROM
        ({{ dbt_utils.union_relations(relations=tables_list, exclude=['_dbt_source_relation']) }})
),

{% for dim in dimensions_count %}
    {{ dim }}_distinct_counts AS (
        SELECT
            {{ dimensions[0] }},
            {% for env in envs %}
                COUNT(DISTINCT (CASE WHEN _dbt_source_relation_no_quotes LIKE '%' || '{{ env }}.' || '%' THEN {{ dim }} END)) AS {{ env }}_{{ dim }}_distinct_count
                {% if not loop.last -%},{% endif -%}
            {% endfor %}                
        FROM
            data
        {%- if filters | length > 0 %}
        WHERE
            {% for i in range(filters|length) -%}
                {{ filters[i] }}
                {% if not loop.last -%}
                    AND
                {% endif -%}
            {%- endfor -%}
        {% endif %}
        {{ dbt_utils.group_by(n=1) }}
    ),
{% endfor %}

{% for dim in dimensions_count %}
    {{ dim }}_null_counts AS (
        SELECT
            {{ dimensions[0] }},
            {% for env in envs %}
                SUM(CASE WHEN _dbt_source_relation_no_quotes LIKE '%' || '{{ env }}.' || '%' AND {{ dim }} IS NULL THEN 1 ELSE 0 END) AS {{ env }}_{{ dim }}_null_count
                {% if not loop.last -%},{% endif -%}
            {% endfor %}                
        FROM
            data
        {%- if filters | length > 0 %}
        WHERE
            {% for i in range(filters|length) -%}
                {{ filters[i] }}
                {% if not loop.last -%}
                    AND
                {% endif -%}
            {%- endfor -%}
    {% endif %}
        {{ dbt_utils.group_by(n=1) }}
    ),
{% endfor %}

final_data AS (
    SELECT
        _dbt_source_relation_no_quotes AS _dbt_source_relation,
        {{ dimension_selection }}
        COUNT(*) AS count,
        {{ column_selection }}
    FROM
        data
    {%- if filters | length > 0 %}
        WHERE
            {% for i in range(filters|length) -%}
                {{ filters[i] }}
                {% if not loop.last -%}
                    AND
                {% endif -%}
            {%- endfor -%}
    {% endif %}
    {{ dbt_utils.group_by(n=group_length) }}
),

transposed_data AS (
    SELECT 
        {{ alias_selection }}
        {{ dimension_counts }}
        {% for table_relation in table_relation_info -%}
            {%- for column_name in metrics %}
                ROUND(COALESCE(SUM(CASE WHEN _dbt_source_relation = '{{ table_relation }}' THEN sum_{{ column_name }} END), 0), 2) AS {{ table_relation.split('.')[1] }}_{{ column_name }}
                {%- if not loop.last -%},{%- endif -%}
            {%- endfor -%}
            {%- if not loop.last -%},{%- endif -%}
        {%- endfor %}
    FROM
        final_data d
    {%- for dim in dimensions_count %}
        LEFT JOIN {{ dim }}_distinct_counts cd{{ loop.index }} ON d.{{ dimensions[0].split(' as ')[1] }} = cd{{ loop.index }}.{{ dimensions[0].split(' as ')[1] }}
        {%- if loop.last -%}
        {%- endif -%}
    {%- endfor %}
    {%- for dim in dimensions_count %}
        LEFT JOIN {{ dim }}_null_counts cn{{ loop.index }} ON d.{{ dimensions[0].split(' as ')[1] }} = cn{{ loop.index }}.{{ dimensions[0].split(' as ')[1] }}
        {%- if loop.last -%}
        {%- endif -%}
    {%- endfor %}
    {{ dbt_utils.group_by(n=dimensions|length + dimensions_count|length * 4) }}
),

data_checks AS (
    SELECT
        {{ alias_selection }}
        {% for i in range(0, dimensions_env|length, 2) -%}
            COALESCE({{ dimensions_env[i] }}, 0)::NUMERIC(30,6) AS {{ dimensions_env[i] }},
            COALESCE({{ dimensions_env[i+1] }}, 0)::NUMERIC(30,6) AS {{ dimensions_env[i+1] }},
            CASE
                WHEN {{ dimensions_env[i+1] }} >= {{ dimensions_env[i] }} THEN 0
                ELSE 1
            END AS check_dim_{{ (i/2) | int }},
            {%- if not loop.last -%}
            {%- endif -%}    
        {%- endfor %}
        {% for i in range(0, metrics_env|length, 2) -%}
            COALESCE({{ metrics_env[i] }}, 0)::NUMERIC(30,6) AS {{ metrics_env[i] }},
            COALESCE({{ metrics_env[i+1] }}, 0)::NUMERIC(30,6) AS {{ metrics_env[i+1] }},
            CASE
                WHEN COALESCE({{ metrics_env[i] }}, 0) < COALESCE({{ metrics_env[i+1] }}, 0) AND COALESCE({{ metrics_env[i] }}, 0) = 0 THEN 100
                ELSE ABS(COALESCE(((({{ metrics_env[i] }} - {{ metrics_env[i+1] }})*100) / NULLIF({{ metrics_env[i] }}, 0)), 0)::NUMERIC(30,6))
            END as diff_metric_{{ (i/2) | int }},
            CASE WHEN COALESCE({{ metrics_env[i] }}, 0) < COALESCE({{ metrics_env[i+1] }}, 0) AND COALESCE({{ metrics_env[i] }}, 0) = 0 THEN 1
            ELSE
                CASE
                    WHEN ABS(COALESCE(((({{ metrics_env[i] }} - {{ metrics_env[i+1] }})*100) / NULLIF({{ metrics_env[i] }}, 0)), 0))::NUMERIC(30,6) <= 2 THEN 0
                    ELSE 1
                END
            END AS check_metric_{{ (i/2) | int }}
        {%- if not loop.last -%},
        {%- endif -%}
        {%- endfor %}
    FROM
        transposed_data d
)

SELECT
    CASE WHEN
        {%- for i in range(0, metrics_env|length, 2) %}
            check_metric_{{ (i/2) | int }} {%- if not loop.last -%}+{%- endif -%}
        {%- endfor %} >= 1
        THEN '⨉'
    ELSE
        '✔'
    END AS metric_check,
    CASE WHEN
        {%- for i in range(0, dimensions_env|length, 2) %}
            check_dim_{{ (i/2) | int }} {%- if not loop.last -%}+{%- endif -%}
        {%- endfor %} >= 1
        THEN '⨉'
    ELSE
        '✔'
    END AS dimensions_check,
    *
FROM
    data_checks
ORDER BY 3 DESC

{%- endmacro -%}
