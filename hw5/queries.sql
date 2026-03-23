USE requestsdb;

-- successful vs unsuccessful
SELECT
  SUM(CASE WHEN response_code = 200 THEN 1 ELSE 0 END) AS successful_requests,
  SUM(CASE WHEN response_code <> 200 THEN 1 ELSE 0 END) AS unsuccessful_requests
FROM requests;

-- requests from banned countries
SELECT COUNT(*) AS banned_country_requests
FROM requests
WHERE is_banned = TRUE;

-- Male vs Female
SELECT gender, COUNT(*) AS request_count
FROM requests
GROUP BY gender
ORDER BY request_count DESC;

-- top 5 countries
SELECT country, COUNT(*) AS request_count
FROM requests
GROUP BY country
ORDER BY request_count DESC
LIMIT 5;

-- age group with most requests
SELECT age_group, request_count
FROM (
  SELECT
    CASE
      WHEN age IS NULL THEN 'unknown'
      WHEN age < 18 THEN '<18'
      WHEN age BETWEEN 18 AND 25 THEN '18-25'
      WHEN age BETWEEN 26 AND 35 THEN '26-35'
      WHEN age BETWEEN 36 AND 50 THEN '36-50'
      ELSE '50+'
    END AS age_group,
    COUNT(*) AS request_count
  FROM requests
  GROUP BY age_group
) t
ORDER BY request_count DESC
LIMIT 1;

-- income group with most requests
SELECT income_group, request_count
FROM (
  SELECT
    CASE
      WHEN income IS NULL THEN 'unknown'
      WHEN income < 25000 THEN '<25k'
      WHEN income BETWEEN 25000 AND 49999 THEN '25k-49k'
      WHEN income BETWEEN 50000 AND 99999 THEN '50k-99k'
      ELSE '100k+'
    END AS income_group,
    COUNT(*) AS request_count
  FROM requests
  GROUP BY income_group
) t
ORDER BY request_count DESC
LIMIT 1;

-- optional: verify failed table
SELECT error_code, COUNT(*) AS cnt
FROM failed_requests
GROUP BY error_code
ORDER BY cnt DESC;
