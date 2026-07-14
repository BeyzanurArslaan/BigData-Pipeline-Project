with source_products as (
    select
        cast(product_id as string) as product_id,
        cast(product_category_name as string) as product_category_name,
        cast(product_name_lenght as integer) as product_name_length,
        cast(product_description_lenght as integer) as product_description_length,
        cast(product_photos_qty as integer) as product_photos_qty,
        cast(product_weight_g as double) as product_weight_g,
        cast(product_length_cm as double) as product_length_cm,
        cast(product_height_cm as double) as product_height_cm,
        cast(product_width_cm as double) as product_width_cm,
        row_number() over (
            partition by cast(product_id as string)
            order by
                cast(product_category_name as string) asc nulls last,
                cast(product_name_lenght as integer) asc nulls last,
                cast(product_description_lenght as integer) asc nulls last,
                cast(product_photos_qty as integer) asc nulls last,
                cast(product_weight_g as double) asc nulls last,
                cast(product_length_cm as double) asc nulls last,
                cast(product_height_cm as double) asc nulls last,
                cast(product_width_cm as double) asc nulls last
        ) as row_number
    from {{ ref('stg_products') }}
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
from source_products
where row_number = 1
