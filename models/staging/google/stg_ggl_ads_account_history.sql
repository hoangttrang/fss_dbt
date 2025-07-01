WITH account_history AS (
        SELECT * FROM  {{ source ('google_ads', 'account_history')}}
)

SELECT 
    CAST(id AS BIGINT) AS id,
    CAST(updated_at AS timestamp with time zone) AS updated_at,
    CAST(manager_customer_id AS bigint) AS manager_customer_id,
    CAST(auto_tagging_enabled AS boolean) AS auto_tagging_enabled,
    CAST(currency_code AS character varying) AS currency_code,
    CAST(descriptive_name AS character varying) AS descriptive_name,
    CAST(final_url_suffix AS character varying) AS final_url_suffix,
    CAST(hidden AS boolean) AS hidden,
    CAST(manager AS boolean) AS manager,
    CAST(optimization_score AS double precision) AS optimization_score,
    CAST(pay_per_conversion_eligibility_failure_reasons AS character varying) AS pay_per_conversion_eligibility_failure_reasons,
    CAST(test_account AS boolean) AS test_account,
    CAST(time_zone AS character varying) AS time_zone,
    CAST(_fivetran_synced AS timestamp with time zone) AS _fivetran_synced,
    CAST(_fivetran_start AS timestamp with time zone) AS _fivetran_start,
    CAST(_fivetran_end AS timestamp with time zone) AS _fivetran_end,
    CAST(_fivetran_active AS boolean) AS _fivetran_active,
    CAST(tracking_url_template AS character varying) AS tracking_url_template
FROM account_history