WITH dataset AS (
  SELECT
    facets.email_channel.mail_event.mail.destination as emailaddresses,
    event_type,
    arrival_timestamp
  FROM "pinpoint-events"."[REENGAGEMENT_S3_TABLENAME]"
  WHERE
    (event_type = '_email.delivered' AND from_unixtime(arrival_timestamp/1000) > date_add('month', -10, current_date))
   OR
    (event_type IN ('_email.open', '_email.click') AND from_unixtime(arrival_timestamp/1000) > date_add('month', -6, current_date))
),
delivered AS (
  SELECT
    emailaddresses,
    'DELIVERED' AS event_type,
    arrival_timestamp
  FROM dataset
  WHERE event_type = '_email.delivered'
),
engaged AS (
  SELECT
    emailaddresses,
    'ENGAGEMENT' AS event_type,
    arrival_timestamp
  FROM dataset
  WHERE event_type IN ('_email.open', '_email.click')

),
joined AS (
  SELECT
    emailaddresses,
    arrival_timestamp AS delivered_timestamp,
    null AS engagement_timestamp
  FROM delivered
  UNION
  SELECT
    emailaddresses,
    null AS delivered_timestamp,
    arrival_timestamp AS engagement_timestamp
  FROM engaged
),
flatten AS (
  SELECT emailaddress, delivered_timestamp, engagement_timestamp
  FROM joined
  CROSS JOIN UNNEST(emailaddresses) as t(emailaddress)
),
min_max AS (
  SELECT
    emailaddress,
    MAX(delivered_timestamp) AS max_delivered_timestamp,
    MIN(delivered_timestamp) AS min_delivered_timestamp,
    MAX(engagement_timestamp) AS last_engagement,
    COUNT(delivered_timestamp) AS num_delivered
  FROM flatten
  GROUP BY emailaddress
)
SELECT
  'EMAIL' AS ChannelType,
  emailaddress AS Id,
  emailaddress AS Address,
  max_delivered_timestamp AS "Attributes.max_delivered_timestamp",
  min_delivered_timestamp AS "Attributes.min_delivered_timestamp",
  last_engagement AS "Attributes.last_engagement",
  num_delivered AS "Attributes.num_delivered"
FROM min_max
WHERE last_engagement IS NULL -- No Engagement in the last 6 months
  AND from_unixtime(min_delivered_timestamp/1000) < date_add('month', -6, current_date) -- Emails started more than 6 months ago
  AND num_delivered > 5 -- Has at least 5 delivered emails
