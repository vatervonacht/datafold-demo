with org_events as (
    select *
    from {{ ref('dim_orgs') }}
    left join {{ ref('feature_used') }} using (org_id)
    where sub_plan is null
),

final as (
    select distinct
        org_id,
        count(*) as usage
    from org_events
    where
        -- select orgs created within the last 60 days, with usage within the 30 days
        event_timestamp::date > ('2022-11-01'::date - 30)
        and created_at::date > ('2022-11-01'::date - 60)
    group by 1
)

select * from final
