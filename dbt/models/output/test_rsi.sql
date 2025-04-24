WITH base AS (
    SELECT
        SYMBOL,
        DATE,
        CLOSE,
        LAG(CLOSE) OVER (
            PARTITION BY SYMBOL 
            ORDER BY DATE
        ) AS PREV_CLOSE
    FROM {{ source('LAB', 'STOCK_DATA') }}
),

gains_losses AS (
    SELECT
        SYMBOL,
        DATE,
        CLOSE,
        AVG(
            CASE 
                WHEN CLOSE - PREV_CLOSE > 0 THEN CLOSE - PREV_CLOSE 
                ELSE 0 
            END
        ) OVER (
            PARTITION BY SYMBOL 
            ORDER BY DATE 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS AVG_GAIN,
        AVG(
            CASE 
                WHEN CLOSE - PREV_CLOSE < 0 THEN ABS(CLOSE - PREV_CLOSE) 
                ELSE 0 
            END
        ) OVER (
            PARTITION BY SYMBOL 
            ORDER BY DATE 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS AVG_LOSS
    FROM base
)

SELECT
    SYMBOL,
    DATE,
    CLOSE,
    AVG_GAIN,
    AVG_LOSS,
    CASE 
        WHEN AVG_LOSS = 0 THEN 100
        ELSE 100 - (100 / (1 + (AVG_GAIN / AVG_LOSS)))
    END AS RSI_14
FROM gains_losses