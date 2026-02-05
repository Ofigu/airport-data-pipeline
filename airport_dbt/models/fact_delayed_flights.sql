{{ config(
    materialized='incremental',
    unique_key='flight_id',
    incremental_strategy='merge',
    file_format='delta'
) }}

WITH flights_delays As (
    SELECT
        CONCAT(
            airline_code,
            flight_number,
            arrival_departure,
            DATE_FORMAT(flight_date, 'yyyy-MM-dd')
        ) AS flight_id,
        flight_number,
        airline_code,
        airline_name,
        flight_date,
        scheduled_landing_time,
        actual_landing_time,

        CASE
            WHEN actual_landing_time IS NOT NULL
                AND status_en = 'LANDED'
                AND actual_landing_time != scheduled_landing_time
            THEN DATEDIFF(MINUTE, scheduled_landing_time, actual_landing_time)
            ELSE NULL
        END AS delay_minutes

FROM {{ source('silver', 'flights_clean')}}
WHERE status_en = 'LANDED'

{% if is_incremental() %}
    -- Only process new or updated records
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
)

SELECT * FROM flights_delays