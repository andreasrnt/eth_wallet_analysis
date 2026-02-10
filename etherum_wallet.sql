WITH filtered AS (
  SELECT
    DATE(block_timestamp) AS day,
    from_address AS wallet,
    SAFE_CAST(value AS BIGNUMERIC) AS value_bn,
    token_address
  FROM `bigquery-public-data.crypto_ethereum.token_transfers`
  WHERE block_timestamp BETWEEN '2023-01-01' AND '2023-01-31'
    AND token_address IN (
      '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', -- USDC
      '0xdac17f958d2ee523a2206206994597c13d831ec7', -- USDT
      '0x6b175474e89094c44da98b954eedeac495271d0f'  -- DAI
    )
    AND MOD(ABS(FARM_FINGERPRINT(from_address)), 100) < 5
)

, stablecoin_transfers_limited AS (
SELECT
  day,
  wallet,
  COUNT(*) AS transfer_count,
  SUM(
    CASE
      WHEN token_address IN (
        '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
        '0xdac17f958d2ee523a2206206994597c13d831ec7'
      ) THEN value_bn / 1e6
      ELSE value_bn / 1e18
    END
  ) AS stablecoin_amount
FROM filtered
GROUP BY day, wallet
)

, eth_activity_limited AS (
SELECT
  DATE(block_timestamp) AS day,
  from_address AS wallet,
  COUNT(*) AS eth_tx_count
FROM `bigquery-public-data.crypto_ethereum.transactions`
WHERE block_timestamp BETWEEN '2023-01-01' AND '2023-01-31'
  AND MOD(ABS(FARM_FINGERPRINT(from_address)), 100) < 5
GROUP BY day, wallet
)

, final AS (
SELECT
  COALESCE(s.day, e.day) AS day,
  COALESCE(s.wallet, e.wallet) AS wallet,
  IFNULL(s.transfer_count, 0) AS stablecoin_tx_count,
  IFNULL(s.stablecoin_amount, 0) AS stablecoin_amount,
  IFNULL(e.eth_tx_count, 0) AS eth_tx_count
FROM stablecoin_transfers_limited s
FULL OUTER JOIN eth_activity_limited e
  ON s.wallet = e.wallet
 AND s.day = e.day
)
;
