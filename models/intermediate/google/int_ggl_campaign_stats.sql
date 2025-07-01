

with base as (

    select * 
    from {{ ref('stg_ggl_ads_campaign_stats')}}
),

fields as (

    select
        
    
    
    _fivetran_id
    
 as 
    
    _fivetran_id
    
, 
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    
    
    ad_network_type
    
 as 
    
    ad_network_type
    
, 
    
    
    clicks
    
 as 
    
    clicks
    
, 
    
    
    cost_micros
    
 as 
    
    cost_micros
    
, 
    
    
    customer_id
    
 as 
    
    customer_id
    
, 
    
    
    date
    
 as 
    
    date
    
, 
    
    
    device
    
 as 
    
    device
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    impressions
    
 as 
    
    impressions
    
, 
    
    
    conversions
    
 as 
    
    conversions
    
, 
    
    
    conversions_value
    
 as 
    
    conversions_value
    
, 
    
    
    view_through_conversions
    
 as 
    
    view_through_conversions
    



    
        


, cast('' as VARCHAR) as source_relation




    from base
),

final as (

    select
        source_relation, 
        customer_id as account_id, 
        date as date_day, 
        id as campaign_id, 
        ad_network_type,
        device,
        coalesce(clicks, 0) as clicks, 
        coalesce(cost_micros, 0) / 1000000.0 as spend, 
        coalesce(impressions, 0) as impressions,
        coalesce(conversions, 0) as conversions,
        coalesce(conversions_value, 0) as conversions_value,
        coalesce(view_through_conversions, 0) as view_through_conversions
        
        





    from fields
)

select *
from final