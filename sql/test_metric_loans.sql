WITH 
-- CTE 1: Average loan amount per loan type
AvgLoanAmount AS (
    SELECT 
        loan_type,
        AVG(loan_amount) AS average_amount
    FROM 
        loans
    GROUP BY 
        loan_type
),
-- CTE 2: Oldest applicant per loan type
OldestApplicant AS (
    SELECT 
        loan_type,
        MAX(applicant_age) AS oldest_age
    FROM 
        loans
    GROUP BY 
        loan_type
),
-- CTE 3: Youngest applicant per loan type
YoungestApplicant AS (
    SELECT 
        loan_type,
        MIN(applicant_age) AS youngest_age
    FROM 
        loans
    GROUP BY 
        loan_type
),
-- CTE 4: Total loans taken this year
LoansThisYear AS (
    SELECT 
        loan_type,
        COUNT(loan_id) AS loans_this_year_count
    FROM 
        loans
    WHERE 
        SUBSTR(application_date, 1, 4) = EXTRACT(YEAR FROM CURRENT_DATE)
    GROUP BY 
        loan_type
),
-- CTE 5: Average interest rate for loans taken by applicants under 30
AvgInterestUnder30 AS (
    SELECT 
        AVG(interest_rate) AS avg_interest
    FROM 
        loans
    WHERE 
        applicant_age < 30
)

-- Main Query joining the CTEs
SELECT 
    ala.loan_type,
    ala.average_amount,
    oa.oldest_age,
    ya.youngest_age,
    ly.loans_this_year_count,
    aiu.avg_interest AS avg_interest_for_under_30
FROM 
    AvgLoanAmount ala
LEFT JOIN 
    OldestApplicant oa ON ala.loan_type = oa.loan_type
LEFT JOIN 
    YoungestApplicant ya ON ala.loan_type = ya.loan_type
LEFT JOIN 
    LoansThisYear ly ON ala.loan_type = ly.loan_type
CROSS JOIN 
    AvgInterestUnder30 aiu
ORDER BY 
    ala.loan_type;
