with source_sellers as (
    select
        cast(seller_id as string) as seller_id,
        cast(seller_zip_code_prefix as integer) as seller_zip_code_prefix,
        cast(seller_city as string) as seller_city,
        cast(seller_state as string) as seller_state,
        row_number() over (
            partition by cast(seller_id as string)
            order by
                cast(seller_zip_code_prefix as integer) asc nulls last,
                cast(seller_city as string) asc nulls last,
                cast(seller_state as string) asc nulls last
        ) as row_number
    from {{ ref('stg_sellers') }}
)

select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
from source_sellers
where row_number = 1
