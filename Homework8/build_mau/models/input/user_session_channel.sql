WITH session_data AS (
    SELECT
        userId,
        sessionId,
        channel
    FROM {{ source('raw_data', 'user_session_channel') }}
    WHERE sessionId IS NOT NULL
)
SELECT * FROM session_data
