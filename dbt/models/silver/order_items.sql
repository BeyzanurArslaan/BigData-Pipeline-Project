with deduped_order_items as (
    select
        cast(order_id as string) as order_id,
        cast(order_item_id as integer) as order_item_id,
        cast(product_id as string) as product_id,
        cast(seller_id as string) as seller_id,
        cast(shipping_limit_date as timestamp) as shipping_limit_date,
        cast(price as decimal(18, 2)) as price,
        cast(freight_value as decimal(18, 2)) as freight_value,
        row_number() over (
            partition by cast(order_id as string), cast(order_item_id as integer)
            order by
                cast(shipping_limit_date as timestamp) asc nulls last,
                cast(product_id as string) asc nulls last,
                cast(seller_id as string) asc nulls last,
                cast(price as decimal(18, 2)) desc nulls last,
                cast(freight_value as decimal(18, 2)) desc nulls last
        ) as row_number
    from {{ ref('stg_order_items') }}
)

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    cast(
        coalesce(price, cast(0 as decimal(18, 2)))
        + coalesce(freight_value, cast(0 as decimal(18, 2)))
        as decimal(18, 2)
    ) as item_gross_value,
    cast(
        sum(
            coalesce(price, cast(0 as decimal(18, 2)))
            + coalesce(freight_value, cast(0 as decimal(18, 2)))
        ) over (partition by order_id)
        as decimal(18, 2)
    ) as order_items_gross_total
from deduped_order_items
where row_number = 1
