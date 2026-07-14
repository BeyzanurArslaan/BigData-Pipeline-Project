with ranked_sellers as (
    select
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        row_number() over (
            partition by seller_id
            order by
                seller_zip_code_prefix asc nulls last,
                seller_city asc nulls last,
                seller_state asc nulls last
        ) as row_number
    from {{ ref('sellers') }}
)

select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
from ranked_sellers
where row_number = 1
