DROP TABLE IF EXISTS `computer-systems-ritayan-patra.credit_card_info_ritayan.cardholders`;
CREATE OR REPLACE EXTERNAL TABLE `computer-systems-ritayan-patra.credit_card_info_ritayan.cardholders` (
  cardholder_id STRING,
  card_number STRING,
  card_type STRING,
  custoemr_name STRING,
  email STRING,
  phone_number STRING,
  country STRING,
  preferred_currency STRING,
  reward_points INTEGER,
  risk_score NUMERIC
)
OPTIONS (
  format = 'CSV',               -- specify the format of the data
  uris = ['gs://credit_card_data_analysis_ritayan/customers/cardholders.csv'],  -- location of the CSV file in Google Cloud Storage
  skip_leading_rows = 1,        -- skip the header row
  field_delimiter = ','         -- specify the delimiter used in the CSV file
);

SELECT * FROM `computer-systems-ritayan-patra.credit_card_info_ritayan.cardholders` LIMIT 10;