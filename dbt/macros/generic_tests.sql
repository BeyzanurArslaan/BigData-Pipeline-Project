{% test unique_combination_of_columns(model, column_names) %}
select
    {{ column_names | join(', ') }},
    count(*) as record_count
from {{ model }}
group by {{ column_names | join(', ') }}
having count(*) > 1
{% endtest %}

{% test positive_value(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is null
   or {{ column_name }} <= 0
{% endtest %}

{% test non_negative_value(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is null
   or {{ column_name }} < 0
{% endtest %}

{% test value_between(model, column_name, min_value, max_value) %}
select *
from {{ model }}
where {{ column_name }} is null
   or {{ column_name }} < {{ min_value }}
   or {{ column_name }} > {{ max_value }}
{% endtest %}

{% test approximately_equal_sum(model, column_name, compare_model, compare_column, compare_key, tolerance=0.01) %}
with left_total as (
    select coalesce(sum({{ column_name }}), 0) as total_value
    from {{ model }}
),
right_total as (
    select coalesce(sum({{ compare_column }}), 0) as total_value
    from (
        select distinct {{ compare_key }}, {{ compare_column }}
        from {{ compare_model }}
    ) as deduped_compare
)
select *
from left_total
cross join right_total
where abs(left_total.total_value - right_total.total_value) > {{ tolerance }}
{% endtest %}

{% test row_count_matches_relation(model, compare_model) %}
with model_count as (
    select count(*) as row_count
    from {{ model }}
),
compare_count as (
    select count(*) as row_count
    from {{ compare_model }}
)
select *
from model_count
cross join compare_count
where model_count.row_count != compare_count.row_count
{% endtest %}
