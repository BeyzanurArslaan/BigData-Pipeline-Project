with ranked_customers as (
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        row_number() over (
            partition by customer_id
            order by
                customer_unique_id asc nulls last,
                customer_zip_code_prefix asc nulls last,
                customer_city asc nulls last,
                customer_state asc nulls last
        ) as row_number
    from {{ ref('customers') }}
)

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
from ranked_customers
where row_number = 1
