WITH dependent_deduction AS (
    SELECT * FROM  {{ source ('ukg', 'dependent_deduction') }}
)

SELECT
    *
FROM dependent_deduction 

