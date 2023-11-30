with orgs as (
--prod
    select
        org_id,
        min(event_timestamp) as created_at
    from {{ ref('signed_in') }}
    group by 1

-- --dev
--    SELECT
--         org_id
--         , org_name
--         , employee_range
--         , created_at
--     FROM {{ ref('org_created') }}
),

user_count as (
    select
        org_id,
        count(distinct user_id) as num_users
    from {{ ref('user_created') }}
    group by 1
),

subscriptions as (
    select
        org_id,
        event_timestamp as sub_created_at,
        plan as sub_plan,
        price as sub_price
    from {{ ref('subscription_created') }}
)


select
    orgs.org_id,
    orgs.created_at,
    user_count.num_users,
    subscriptions.sub_created_at,
    subscriptions.sub_plan,
    subscriptions.sub_price
from orgs
left join user_count on orgs.org_id = user_count.org_id
left join subscriptions on orgs.org_id = subscriptions.org_id
