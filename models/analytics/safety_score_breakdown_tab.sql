WITH driving_periods AS (
    SELECT * FROM {{ ref('stg_motive_data_driving_periods') }}
)

, vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
)

, data_vehicle_group_mappings AS (
    SELECT * 
    FROM {{ ref('int_motive_data_driving_period_vehicle_map') }}
    WHERE driving_distance >= 0::double precision 
       AND driving_distance <= 500::double precision 
       AND (not_current IS FALSE OR not_current IS NULL) 
)

, combined_events AS (
    SELECT * FROM {{ ref('stg_motive_data_combined_events') }}
)

, combined_events_group_mappings AS ( 
    SELECT combined_events.id,
        combined_events.event_id,
        combined_events.type,
        combined_events.driver_id,
        combined_events.driver_first_name,
        combined_events.driver_last_name,
        combined_events.vehicle_id,
        combined_events.coaching_status, 
        combined_events.start_date,
        combined_events.severity,
        combined_events.group_id,
        combined_events.month,
        combined_events.created_at,
        combined_events.updated_at,
        combined_events.max_over_speed_in_kph,
        combined_events.max_over_speed_in_mph,
        combined_events.not_current, 
        vehicle_map_rs.region,
        vehicle_map_rs.translated_site,
        vehicle_map_rs.group_name
    FROM combined_events
    JOIN vehicle_map_rs
        ON combined_events.vehicle_id = vehicle_map_rs.vehicle_id
    WHERE translated_site IS NOT NULL
    AND (combined_events.not_current IS FALSE OR combined_events.not_current IS NULL) 
)

, drive_distances AS (
    SELECT 
        EXTRACT(year FROM a.start_date) AS year,
        EXTRACT(quarter FROM a.start_date) AS quarter,
        to_char(to_date(EXTRACT(month FROM a.start_date)::text, 'MM'::text)::timestamp with time zone, 'Mon'::text) AS month,
        EXTRACT(week FROM a.start_date) AS week,
        to_char(a.start_date, 'Day'::text) AS day,
        a.start_date::date AS date,
        a.region,
        a.group_name,
        a.translated_site AS site_name,
        COALESCE(a.driver_id, '-1'::integer) AS driver_id,
        COALESCE(a.driver_first_name, 'unknown'::character varying) AS driver_first_name,
        COALESCE(a.driver_last_name, 'unknown'::character varying) AS driver_last_name,
        (COALESCE(a.driver_first_name, 'unknown'::character varying)::text || ' '::text) || COALESCE(a.driver_last_name, 'unknown'::character varying)::text AS driver_full_name,
        COUNT(DISTINCT a.event_id) AS trips,
        ROUND(sum(a.driving_distance)::numeric, 2) AS miles_driven
    FROM data_vehicle_group_mappings a
    WHERE a.start_date >= '2023-01-01 06:00:00+00'::timestamp with time zone AND a.start_date <= CURRENT_DATE
    GROUP BY (EXTRACT(year FROM a.start_date)), (EXTRACT(quarter FROM a.start_date)), (to_char(to_date(EXTRACT(month FROM a.start_date)::text, 'MM'::text)::timestamp with time zone, 'Mon'::text)), (EXTRACT(week FROM a.start_date)), (to_char(a.start_date, 'Day'::text)), (a.start_date::date), a.region, a.group_name, a.translated_site, (COALESCE(a.driver_id, '-1'::integer)), (COALESCE(a.driver_first_name, 'unknown'::character varying)), (COALESCE(a.driver_last_name, 'unknown'::character varying)), ((COALESCE(a.driver_first_name, 'unknown'::character varying)::text || ' '::text) || COALESCE(a.driver_last_name, 'unknown'::character varying)::text)
)

, event_breakdown AS (
    SELECT 
        EXTRACT(year FROM a.start_date) AS year,
        EXTRACT(quarter FROM a.start_date) AS quarter,
        to_char(to_date(EXTRACT(month FROM a.start_date)::text, 'MM'::text)::timestamp with time zone, 'Mon'::text) AS month,
        EXTRACT(week FROM a.start_date) AS week,
        to_char(a.start_date, 'Day'::text) AS day,
        start_date::date AS date,
        a.region,
        a.group_name,
        a.translated_site AS site_name,
        COALESCE(a.driver_id, '-1'::integer) AS driver_id,
        COALESCE(a.driver_first_name, 'unknown'::character varying) AS driver_first_name,
        COALESCE(a.driver_last_name, 'unknown'::character varying) AS driver_last_name,
        (COALESCE(a.driver_first_name, 'unknown'::character varying)::text || ' '::text) || COALESCE(a.driver_last_name, 'unknown'::character varying)::text AS driver_full_name
        {% for event_type in get_motive_event_type() -%}
            {%- if event_type|lower == 'speeding' %}
            -- Speeding 0–5 mph
            , SUM(
                CASE 
                    WHEN type = '{{ event_type }}'
                        AND max_over_speed_in_mph >= 0 AND max_over_speed_in_mph < 5 
                    THEN 1 
                    ELSE 0 
                END
                ) AS speeding
            -- Speeding 5–10 mph
            , SUM(
                CASE 
                    WHEN type = '{{ event_type }}'
                        AND (max_over_speed_in_mph >= 5 AND max_over_speed_in_mph < 6)
                    THEN 1 
                    ELSE 0 
                END
                ) AS speeding_five

            -- Speeding 6–10 mph
            , SUM(
                CASE 
                    WHEN type = '{{ event_type }}'
                        AND (max_over_speed_in_mph >= 6 AND max_over_speed_in_mph < 10)
                    THEN 1 
                    ELSE 0 
                END
                ) AS speeding_six_to_ten

            -- Speeding 10–15 mph
            , SUM(
                CASE 
                    WHEN type = '{{ event_type }}'
                        AND (max_over_speed_in_mph >= 10 AND max_over_speed_in_mph < 15)
                    THEN 1 
                    ELSE 0 
                END
                ) AS speeding_ten_to_fifteen
            -- Speeding 15+ mph
            , SUM(
                CASE 
                    WHEN type = '{{ event_type }}'
                        AND max_over_speed_in_mph >= 15
                    THEN 1 
                    ELSE 0 
                END
                ) AS speeding_fifteen_plus
            {%- else -%}
            -- For all other event types, just do a single column
            , SUM(
                CASE
                    WHEN type = '{{ event_type }}'
                    THEN 1
                    ELSE 0
                END
                ) AS {{ event_type }}
            {%- endif -%}

        {%- endfor %}

        , COUNT(DISTINCT a.event_id) AS total_events
        FROM combined_events_group_mappings a
        WHERE 1 = 1 
        AND a.coaching_status::text <> 'uncoachable'::text AND a.start_date >= '2023-01-01 06:00:00+00'::timestamp with time zone AND a.start_date <= CURRENT_DATE
        GROUP BY (EXTRACT(year FROM a.start_date)), (EXTRACT(quarter FROM a.start_date)), (to_char(to_date(EXTRACT(month FROM a.start_date)::text, 'MM'::text)::timestamp with time zone, 'Mon'::text)), (EXTRACT(week FROM a.start_date)), (to_char(a.start_date, 'Day'::text)), (a.start_date::date), a.region, a.group_name, a.translated_site, (COALESCE(a.driver_id, '-1'::integer)), (COALESCE(a.driver_first_name, 'unknown'::character varying)), (COALESCE(a.driver_last_name, 'unknown'::character varying)), ((COALESCE(a.driver_first_name, 'unknown'::character varying)::text || ' '::text) || COALESCE(a.driver_last_name, 'unknown'::character varying)::text)
    )

, combined_dd_and_eb AS (
    SELECT 
        dd.year,
        dd.quarter,
        dd.month,
        dd.week,
        dd.day,
        dd.date,
        dd.region,
        dd.group_name,
        dd.site_name,
        dd.driver_id,
        dd.driver_first_name,
        dd.driver_last_name,
        dd.driver_full_name,
        dd.trips,
        dd.miles_driven,
        COALESCE(eb.speeding, 0::bigint) AS speeding,
        COALESCE(eb.speeding_five, 0::bigint) AS speeding_five,
        COALESCE(eb.speeding_six_to_ten, 0::bigint) AS speeding_six_to_ten,
        COALESCE(eb.speeding_ten_to_fifteen, 0::bigint) AS speeding_ten_to_fifteen,
        COALESCE(eb.speeding_fifteen_plus, 0::bigint) AS speeding_fifteen_plus,
        COALESCE(eb.camera_obstruction, 0::bigint) AS camera_obstruction,
        COALESCE(eb.cell_phone, 0::bigint) AS cell_phone,
        COALESCE(eb.crash, 0::bigint) AS crash,
        COALESCE(eb.distraction, 0::bigint) AS distraction,
        COALESCE(eb.driver_facing_cam_obstruction, 0::bigint) AS driver_facing_cam_obstruction,
        COALESCE(eb.drowsiness, 0::bigint) AS drowsiness,
        COALESCE(eb.forward_collision_warning, 0::bigint) AS forward_collision_warning,
        COALESCE(eb.manual_event, 0::bigint) AS manual_event,
        COALESCE(eb.near_miss, 0::bigint) AS near_miss,
        COALESCE(eb.ran_a_red_light, 0::bigint) AS ran_a_red_light,
        COALESCE(eb.road_facing_cam_obstruction, 0::bigint) AS road_facing_cam_obstruction,
        COALESCE(eb.seat_belt_violation, 0::bigint) AS seat_belt_violation,
        COALESCE(eb.stop_sign_violation, 0::bigint) AS stop_sign_violation,
        COALESCE(eb.tailgating, 0::bigint) AS tailgating,
        COALESCE(eb.unsafe_lane_change, 0::bigint) AS unsafe_lane_change,
        COALESCE(eb.total_events, 0::bigint) AS total_events
        FROM drive_distances dd
        LEFT JOIN event_breakdown eb 
        ON eb.year = dd.year AND eb.month = dd.month AND eb.quarter = dd.quarter AND eb.week = dd.week AND eb.date = dd.date AND eb.group_name::text = dd.group_name::text AND eb.site_name::text = dd.site_name::text AND eb.driver_id = dd.driver_id
)

, safety_score_breakdown AS (
    SELECT cddeb.year,
    cddeb.quarter,
    cddeb.month,
    cddeb.week,
    cddeb.day,
    cddeb.date,
    cddeb.region,
    cddeb.group_name,
    cddeb.site_name,
    cddeb.driver_id,
    cddeb.driver_first_name,
    cddeb.driver_last_name,
    cddeb.driver_full_name,
    cddeb.trips,
    cddeb.miles_driven,
    cddeb.speeding,
    cddeb.speeding_five,
    cddeb.speeding_six_to_ten,
    cddeb.speeding_ten_to_fifteen,
    cddeb.speeding_fifteen_plus,
    cddeb.camera_obstruction,
    cddeb.cell_phone,
    cddeb.crash,
    cddeb.distraction,
    cddeb.driver_facing_cam_obstruction,
    cddeb.drowsiness,
    cddeb.forward_collision_warning,
    cddeb.manual_event,
    cddeb.near_miss,
    cddeb.ran_a_red_light,
    cddeb.road_facing_cam_obstruction,
    cddeb.seat_belt_violation,
    cddeb.stop_sign_violation,
    cddeb.tailgating,
    cddeb.unsafe_lane_change,
    cddeb.total_events,
    ssw.points_speeding,
    ssw.points_speeding_five,
    ssw.points_speeding_six_to_ten,
    ssw.points_speeding_ten_to_fifteen,
    ssw.points_speeding_fifteen_plus,
    ssw.points_camera_obstruction,
    ssw.points_cell_phone,
    ssw.points_crash,
    ssw.points_distraction,
    ssw.points_driver_facing_cam_obstruction,
    ssw.points_drowsiness,
    ssw.points_forward_collision_warning,
    ssw.points_manual_event,
    ssw.points_near_miss,
    ssw.points_ran_a_red_light,
    ssw.points_road_facing_cam_obstruction,
    ssw.points_seat_belt_violation,
    ssw.points_stop_sign_violation,
    ssw.points_tailgating,
    ssw.points_unsafe_lane_change,
    cddeb.speeding * ssw.points_speeding AS total_speeding,
    cddeb.speeding_five * ssw.points_speeding_five AS total_speeding_five,
    cddeb.speeding_six_to_ten * ssw.points_speeding_six_to_ten AS total_speeding_six_to_ten,
    cddeb.speeding_ten_to_fifteen * ssw.points_speeding_ten_to_fifteen AS total_speeding_ten_to_fifteen,
    cddeb.speeding_fifteen_plus * ssw.points_speeding_fifteen_plus AS total_speeding_fifteen_plus,
    cddeb.camera_obstruction * ssw.points_camera_obstruction AS total_camera_obstruction,
    cddeb.cell_phone * ssw.points_cell_phone AS total_cell_phone,
    cddeb.crash * ssw.points_crash AS total_crash,
    cddeb.distraction * ssw.points_distraction AS total_distraction,
    cddeb.driver_facing_cam_obstruction * ssw.points_driver_facing_cam_obstruction AS total_driver_facing_cam_obstruction,
    cddeb.drowsiness * ssw.points_drowsiness AS total_drowsiness,
    cddeb.forward_collision_warning * ssw.points_forward_collision_warning AS total_forward_collision_warning,
    cddeb.manual_event * ssw.points_manual_event AS total_manual_event,
    cddeb.near_miss * ssw.points_near_miss AS total_near_miss,
    cddeb.ran_a_red_light * ssw.points_ran_a_red_light AS total_ran_a_red_light,
    cddeb.road_facing_cam_obstruction * ssw.points_road_facing_cam_obstruction AS total_road_facing_cam_obstruction,
    cddeb.seat_belt_violation * ssw.points_seat_belt_violation AS total_seat_belt_violation,
    cddeb.stop_sign_violation * ssw.points_stop_sign_violation AS total_stop_sign_violation,
    cddeb.tailgating * ssw.points_tailgating AS total_tailgating,
    cddeb.unsafe_lane_change * ssw.points_unsafe_lane_change AS total_unsafe_lane_change,
    cddeb.speeding * ssw.points_speeding + cddeb.speeding_five * ssw.points_speeding_five + cddeb.speeding_six_to_ten * ssw.points_speeding_six_to_ten + cddeb.speeding_ten_to_fifteen * ssw.points_speeding_ten_to_fifteen + cddeb.speeding_fifteen_plus * ssw.points_speeding_fifteen_plus + cddeb.camera_obstruction * ssw.points_camera_obstruction + cddeb.cell_phone * ssw.points_cell_phone + cddeb.crash * ssw.points_crash + cddeb.distraction * ssw.points_distraction + cddeb.driver_facing_cam_obstruction * ssw.points_driver_facing_cam_obstruction + cddeb.drowsiness * ssw.points_drowsiness + cddeb.forward_collision_warning * ssw.points_forward_collision_warning + cddeb.manual_event * ssw.points_manual_event + cddeb.near_miss * ssw.points_near_miss + cddeb.ran_a_red_light * ssw.points_ran_a_red_light + cddeb.road_facing_cam_obstruction * ssw.points_road_facing_cam_obstruction + cddeb.seat_belt_violation * ssw.points_seat_belt_violation + cddeb.stop_sign_violation * ssw.points_stop_sign_violation + cddeb.tailgating * ssw.points_tailgating + cddeb.unsafe_lane_change * ssw.points_unsafe_lane_change AS total_points_for_score,
    cddeb.speeding * 1 + cddeb.speeding_five * 1 + cddeb.speeding_six_to_ten * 1 + cddeb.speeding_ten_to_fifteen * 1 + cddeb.speeding_fifteen_plus * 1 + cddeb.camera_obstruction * 1 + cddeb.cell_phone * 1 + cddeb.crash * 1 + cddeb.distraction * 1 + cddeb.driver_facing_cam_obstruction * 1 + cddeb.drowsiness * 1 + cddeb.forward_collision_warning * 1 + cddeb.manual_event * 1 + cddeb.near_miss * 1 + cddeb.ran_a_red_light * 1 + cddeb.road_facing_cam_obstruction * 1 + cddeb.seat_belt_violation * 1 + cddeb.stop_sign_violation * 1 + cddeb.tailgating * 1 + cddeb.unsafe_lane_change * 1 AS total_points_for_unweighted_score
    FROM combined_dd_and_eb cddeb
    JOIN safety_score_weights ssw 
        ON cddeb.year = ssw.year::numeric AND cddeb.month = ssw.month::text
    )


 SELECT year,
    quarter,
    month,
    week,
    day,
    date,
    region,
    group_name,
    site_name,
    driver_id,
    driver_first_name,
    driver_last_name,
    driver_full_name,
    trips,
    miles_driven,
    speeding,
    speeding_five,
    speeding_six_to_ten,
    speeding_ten_to_fifteen,
    speeding_fifteen_plus,
    camera_obstruction,
    cell_phone,
    crash,
    distraction,
    driver_facing_cam_obstruction,
    drowsiness,
    forward_collision_warning,
    manual_event,
    near_miss,
    ran_a_red_light,
    road_facing_cam_obstruction,
    seat_belt_violation,
    stop_sign_violation,
    tailgating,
    unsafe_lane_change,
    total_events,
    points_speeding,
    points_speeding_five,
    points_speeding_six_to_ten,
    points_speeding_ten_to_fifteen,
    points_speeding_fifteen_plus,
    points_camera_obstruction,
    points_cell_phone,
    points_crash,
    points_distraction,
    points_driver_facing_cam_obstruction,
    points_drowsiness,
    points_forward_collision_warning,
    points_manual_event,
    points_near_miss,
    points_ran_a_red_light,
    points_road_facing_cam_obstruction,
    points_seat_belt_violation,
    points_stop_sign_violation,
    points_tailgating,
    points_unsafe_lane_change,
    total_speeding,
    total_speeding_five,
    total_speeding_six_to_ten,
    total_speeding_ten_to_fifteen,
    total_speeding_fifteen_plus,
    total_camera_obstruction,
    total_cell_phone,
    total_crash,
    total_distraction,
    total_driver_facing_cam_obstruction,
    total_drowsiness,
    total_forward_collision_warning,
    total_manual_event,
    total_near_miss,
    total_ran_a_red_light,
    total_road_facing_cam_obstruction,
    total_seat_belt_violation,
    total_stop_sign_violation,
    total_tailgating,
    total_unsafe_lane_change,
    total_points_for_score,
    total_points_for_unweighted_score,
    CASE
        WHEN miles_driven = 0::numeric THEN 100::numeric
        ELSE round(100::numeric - total_points_for_score::numeric / miles_driven * 1000::numeric, 2)
    END AS safety_score,
    CASE
        WHEN miles_driven = 0::numeric THEN 100::numeric
        ELSE round(100::numeric - total_points_for_unweighted_score::numeric / miles_driven * 1000::numeric, 2)
    END AS safety_score_unweighted,
    now() - '05:00:00'::interval AS as_of_date
FROM safety_score_breakdown ssb
WHERE miles_driven >= 0::numeric