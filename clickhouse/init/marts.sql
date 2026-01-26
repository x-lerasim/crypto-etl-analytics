CREATE DATABASE IF NOT EXISTS crypto;


CREATE TABLE IF NOT EXISTS crypto.dim_assets
(
  asset_id String,
  symbol String,
  asset_name String,
  rank Int32,
  price_usd Decimal(28, 8),
  change_24h Decimal(18, 8),
  market_cap Decimal(38, 2),
  load_datetime DateTime,
  load_date Date
)
ENGINE = ReplacingMergeTree(load_datetime)
PARTITION BY toYYYYMM(load_date)
ORDER BY (load_date, asset_id);

CREATE TABLE IF NOT EXISTS crypto.mart_daily_summary_target
(
  load_date Date,

  total_assets UInt64,
  total_market_cap Decimal(38,2),
  avg_market_cap Decimal(38,2),

  avg_price Decimal(28,8),
  max_price Decimal(28,8),
  min_price Decimal(28,8),

  avg_change_24h Decimal(18,8),

  growing_count UInt64,
  declining_count UInt64,

  top_10_count UInt64,
  top_50_count UInt64,

  strong_growth_count UInt64,
  growth_count UInt64,
  decline_count UInt64,
  strong_decline_count UInt64,

  load_datetime DateTime
)
ENGINE = ReplacingMergeTree(load_datetime)
PARTITION BY toYYYYMM(load_date)
ORDER BY load_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.mv_mart_daily_summary
TO crypto.mart_daily_summary_target
AS
SELECT
  load_date,

  count() as total_assets,
  sum(market_cap) as total_market_cap,
  avg(market_cap) as avg_market_cap,

  avg(price_usd) as avg_price,
  max(price_usd) as max_price,
  min(price_usd) as min_price,

  avg(change_24h) as avg_change_24h,

  countIf(change_24h > 0) as growing_count,
  countIf(change_24h < 0) as declining_count,

  countIf(rank <= 10) as top_10_count,
  countIf(rank <= 50) as top_50_count,

  countIf(change_24h > 10) as strong_growth_count,
  countIf(change_24h > 0 AND change_24h <= 10) as growth_count,
  countIf(change_24h <= 0 AND change_24h > -10) as decline_count,
  countIf(change_24h <= -10) as strong_decline_count,

  now() as load_datetime
FROM crypto.dim_assets
GROUP BY load_date;