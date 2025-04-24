WITH base_data AS (
    SELECT
        SYMBOL,
        DATE,
        CLOSE
    FROM {{ source('LAB', 'STOCK_DATA') }}
),

moving_avg_calc AS (
    SELECT
        SYMBOL,
        DATE,
        CLOSE,

        -- 7-day Moving Average
        AVG(CLOSE) OVER (
            PARTITION BY SYMBOL
            ORDER BY DATE
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS MA_7,

        -- 14-day Moving Average
        AVG(CLOSE) OVER (
            PARTITION BY SYMBOL
            ORDER BY DATE
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS MA_14,

        -- 30-day Moving Average
        AVG(CLOSE) OVER (
            PARTITION BY SYMBOL
            ORDER BY DATE
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS MA_30
    FROM base_data
)

SELECT * FROM moving_avg_calc
