with source_customers as (
    select
        cast(customer_id as string) as customer_id,
        cast(customer_unique_id as string) as customer_unique_id,
        cast(customer_zip_code_prefix as integer) as customer_zip_code_prefix,
        cast(customer_city as string) as customer_city,
        cast(customer_state as string) as customer_state,
        row_number() over (
            partition by cast(customer_id as string)
            order by
                cast(customer_unique_id as string) asc nulls last,
                cast(customer_zip_code_prefix as integer) asc nulls last,
                cast(customer_city as string) asc nulls last,
                cast(customer_state as string) asc nulls last
        ) as row_number
    from {{ ref('stg_customers') }}
)

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
from source_customers
where row_number = 1
