-- Query 1: Successful vs Unsuccessful requests
SELECT 
    'Successful' as request_type,
    COUNT(*) as count
FROM successful_requests
UNION ALL
SELECT 
    'Failed' as request_type,
    COUNT(*) as count
FROM failed_requests;

-- Query 2: Requests from banned countries
SELECT 
    COUNT(*) as banned_country_requests
FROM successful_requests
WHERE is_banned = TRUE;

-- Query 3: Male vs Female requests
SELECT 
    gender,
    COUNT(*) as count
FROM successful_requests
GROUP BY gender
ORDER BY count DESC;

-- Query 4: Top 5 countries
SELECT 
    country,
    COUNT(*) as request_count
FROM successful_requests
GROUP BY country
ORDER BY request_count DESC
LIMIT 5;

-- Query 5: Age group with most requests
SELECT 
    CASE 
        WHEN age BETWEEN 0 AND 17 THEN '0-17'
        WHEN age BETWEEN 18 AND 24 THEN '18-24'
        WHEN age BETWEEN 25 AND 34 THEN '25-34'
        WHEN age BETWEEN 35 AND 44 THEN '35-44'
        WHEN age BETWEEN 45 AND 54 THEN '45-54'
        WHEN age BETWEEN 55 AND 64 THEN '55-64'
        WHEN age >= 65 THEN '65+'
        ELSE 'Unknown'
    END as age_group,
    COUNT(*) as request_count
FROM successful_requests
GROUP BY age_group
ORDER BY request_count DESC
LIMIT 1;

-- Query 6: Income group with most requests
SELECT 
    income,
    COUNT(*) as request_count
FROM successful_requests
GROUP BY income
ORDER BY request_count DESC
LIMIT 1;
