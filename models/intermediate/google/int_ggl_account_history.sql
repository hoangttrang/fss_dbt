with base as (

    select * 
    from {{ ref('stg_ggl_ads_account_history') }}

),

fields as (

    select
        
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    
    
    auto_tagging_enabled
    
 as 
    
    auto_tagging_enabled
    
, 
    
    
    currency_code
    
 as 
    
    currency_code
    
, 
    
    
    descriptive_name
    
 as 
    
    descriptive_name
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    time_zone
    
 as 
    
    time_zone
    
, 
    
    
    updated_at
    
 as 
    
    updated_at
    
, 
    cast(null as boolean) as 
    
    _fivetran_active
    
 


        
    
        


, cast('' as VARCHAR) as source_relation




    from base
),

final as (

    select
        source_relation, 
        id as account_id,
        updated_at,
        currency_code,
        auto_tagging_enabled,
        time_zone,
        descriptive_name as account_name,
        row_number() over (partition by source_relation, id order by updated_at desc) = 1 as is_most_recent_record
    from fields
    where coalesce(_fivetran_active, true)
)

select * 
from final