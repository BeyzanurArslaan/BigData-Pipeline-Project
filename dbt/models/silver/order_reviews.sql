with source_order_reviews as (
    select
        cast(review_id as string) as review_id,
        cast(order_id as string) as order_id,
        cast(review_score as integer) as review_score,
        cast(review_comment_title as string) as review_comment_title,
        cast(review_comment_message as string) as review_comment_message,
        cast(review_creation_date as timestamp) as review_creation_date,
        cast(review_answer_timestamp as timestamp) as review_answer_timestamp,
        coalesce(
            cast(review_id as string),
            concat_ws(
                '||',
                cast(order_id as string),
                cast(review_creation_date as string),
                cast(review_answer_timestamp as string),
                cast(review_score as string)
            )
        ) as review_dedup_key,
        row_number() over (
            partition by coalesce(
                cast(review_id as string),
                concat_ws(
                    '||',
                    cast(order_id as string),
                    cast(review_creation_date as string),
                    cast(review_answer_timestamp as string),
                    cast(review_score as string)
                )
            )
            order by
                cast(review_answer_timestamp as timestamp) asc nulls last,
                cast(review_creation_date as timestamp) asc nulls last,
                cast(order_id as string) asc nulls last
        ) as row_number
    from {{ ref('stg_order_reviews') }}
)

select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,
    review_dedup_key
from source_order_reviews
where row_number = 1
