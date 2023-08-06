SELECT 
    time_request
    , count(1) as number_logins 
FROM
    Logs 
GROUP BY 
    time_request 
ORDER BY 
    number_logins
DESC;
