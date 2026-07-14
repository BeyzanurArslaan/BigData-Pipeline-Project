with ranked_products as (
    select
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        row_number() over (
            partition by product_id
            order by
                product_category_name asc nulls last,
                product_name_length asc nulls last,
                product_description_length asc nulls last,
                product_photos_qty asc nulls last,
                product_weight_g asc nulls last,
                product_length_cm asc nulls last,
                product_height_cm asc nulls last,
                product_width_cm asc nulls last
        ) as row_number
    from {{ ref('products') }}
)

select
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
from ranked_products
where row_number = 1
