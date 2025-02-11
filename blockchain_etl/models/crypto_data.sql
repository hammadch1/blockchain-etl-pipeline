-- models/crypto_data.sql

WITH raw_data AS (
    SELECT * FROM {{ ref('blockchain_data') }}
)

SELECT
    coin_id,
    symbol,
    price,
    market_cap
FROM raw_data
WHERE price > 50000