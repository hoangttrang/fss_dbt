WITH job AS (
    SELECT * FROM  {{ source ('ukg', 'job') }}
)

SELECT
    *
FROM job 

