with order_items as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value,
        item_gross_value,
        order_items_gross_total
    from {{ ref('order_items') }}
),
order_level_payments as (
    select distinct
        order_id,
        order_payment_value,
        primary_payment_type,
        primary_payment_installments
    from {{ ref('order_payments') }}
),
order_reviews as (
    select
        order_id,
        review_score
    from (
        select
            order_id,
            review_score,
            row_number() over (
                partition by order_id
                order by
                    review_creation_date asc nulls last,
                    review_answer_timestamp asc nulls last,
                    review_dedup_key asc nulls last
            ) as row_number
        from {{ ref('order_reviews') }}
    ) as ranked_reviews
    where row_number = 1
)

select
    order_items.order_id,
    order_items.order_item_id,
    orders.customer_id,
    order_items.seller_id,
    order_items.product_id,
    cast(date_format(cast(orders.order_purchase_timestamp as date), 'yyyyMMdd') as integer) as date_key,
    orders.order_status,
    order_items.price,
    order_items.freight_value,
    order_items.item_gross_value,
    order_items.order_items_gross_total,
    order_level_payments.order_payment_value,
    cast(
        case
            when coalesce(order_items.order_items_gross_total, cast(0 as decimal(18, 2))) > 0
                then coalesce(order_level_payments.order_payment_value, cast(0 as decimal(18, 2)))
                    * coalesce(order_items.item_gross_value, cast(0 as decimal(18, 2)))
                    / order_items.order_items_gross_total
            else cast(0 as decimal(18, 2))
        end as decimal(18, 2)
    ) as allocated_payment_value,
    order_level_payments.primary_payment_type,
    order_level_payments.primary_payment_installments,
    order_reviews.review_score
from order_items
left join {{ ref('orders') }} as orders
    on order_items.order_id = orders.order_id
left join order_level_payments
    on order_items.order_id = order_level_payments.order_id
left join order_reviews
    on order_items.order_id = order_reviews.order_id
