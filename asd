SELECT ROW_NUMBER() OVER () AS row_num,
       article_code,
       article_name,
       branch,
       units_sold
FROM  Sales
WHERE article_code IN ( 101, 102, 103 )