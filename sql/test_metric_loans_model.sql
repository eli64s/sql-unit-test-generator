WITH 
-- CTE 1: Average loan amount per loan type
AvgLoanAmount AS (
    SELECT 
        AVG(loan_amount) AS average_amount,
        loan_id,
    FROM 
        loans
    GROUP BY 
        loan_id
),
-- CTE 2: Oldest applicant per loan type
OldestApplicant AS (
    SELECT 
        loan_id,
        MAX(applicant_age) AS oldest_age
    FROM 
        loans
    GROUP BY 
        loan_id
),
-- CTE 3: Youngest applicant per loan type
YoungestApplicant AS (
    SELECT 
        loan_id,
        MIN(applicant_age) AS youngest_age
    FROM 
        loans
    GROUP BY 
        loan_id
),
-- CTE 4: Total loans taken this year
LoansThisYear AS (
    SELECT 
        loan_id,
        COUNT(loan_id) AS loans_this_year_count
    FROM 
        loans
    WHERE 
        SUBSTR(application_date,1,4) = '2022'
    GROUP BY 
        loan_id
),
-- CTE 5: Average interest rate for loans taken by applicants under 30
AvgInterestUnder30 AS (
    SELECT 
        loan_id,
        interest_rate AS avg_interest
    FROM 
        loans
    WHERE 
        applicant_age < 30
)

-- Main Query joining the CTEs
SELECT 
    ala.average_amount,
    aiu.avg_interest AS avg_interest_for_under_30,
    ala.loan_id,
    ly.loans_this_year_count AS loans_this_year_count,
    oa.oldest_age,
    ya.youngest_age
FROM 
    AvgLoanAmount ala
LEFT JOIN 
    OldestApplicant oa ON ala.loan_id = oa.loan_id
LEFT JOIN 
    YoungestApplicant ya ON ala.loan_id = ya.loan_id
LEFT JOIN 
    LoansThisYear ly ON ala.loan_id = ly.loan_id
JOIN 
    AvgInterestUnder30 aiu ON ala.loan_id = aiu.loan_id
WHERE 
    loans_this_year_count > 0
ORDER BY 
    ala.loan_id;