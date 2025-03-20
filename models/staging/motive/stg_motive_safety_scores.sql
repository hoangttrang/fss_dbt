WITH safety_score_weights AS (
    SELECT * FROM {{ source ('citus_motive', 'safety_score_weights') }}
)

SELECT 
    id
    , CAST(year AS INT) AS year
    , month
    , points_speeding
    , points_speeding_five
    , points_speeding_six_to_ten 
    , points_speeding_ten_to_fifteen
    , points_speeding_fifteen_plus 
    , points_camera_obstruction
    , points_cell_phone
    , points_crash
    , points_distraction
    , points_driver_facing_cam_obstruction 
    , points_drowsiness
    , points_forward_collision_warning
    , points_manual_event
    , points_near_miss
    , points_ran_a_red_light
    , points_road_facing_cam_obstruction
    , points_seat_belt_violation 
    , points_stop_sign_violation
    , points_tailgating
    , points_unsafe_lane_change
FROM safety_score_weights