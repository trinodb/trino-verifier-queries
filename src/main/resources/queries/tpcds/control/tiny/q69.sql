SELECT *
FROM
(
  VALUES
    (CHAR 'F', CHAR 'D', CHAR 'Primary             ', BIGINT '1', INTEGER '5000', BIGINT '1', CHAR 'Unknown   ', BIGINT '1'),
    (CHAR 'F', CHAR 'D', CHAR 'Primary             ', BIGINT '1', INTEGER '7500', BIGINT '1', CHAR 'High Risk ', BIGINT '1'),
    (CHAR 'F', CHAR 'D', CHAR 'Secondary           ', BIGINT '1', INTEGER '5000', BIGINT '1', CHAR 'Good      ', BIGINT '1'),
    (CHAR 'F', CHAR 'D', CHAR 'Unknown             ', BIGINT '1', INTEGER '6500', BIGINT '1', CHAR 'Good      ', BIGINT '1'),
    (CHAR 'F', CHAR 'M', CHAR '4 yr Degree         ', BIGINT '1', INTEGER '500', BIGINT '1', CHAR 'Low Risk  ', BIGINT '1'),
    (CHAR 'F', CHAR 'M', CHAR 'College             ', BIGINT '1', INTEGER '6500', BIGINT '1', CHAR 'Low Risk  ', BIGINT '1'),
    (CHAR 'F', CHAR 'M', CHAR 'Primary             ', BIGINT '1', INTEGER '10000', BIGINT '1', CHAR 'Low Risk  ', BIGINT '1'),
    (CHAR 'F', CHAR 'M', CHAR 'Secondary           ', BIGINT '1', INTEGER '5000', BIGINT '1', CHAR 'Good      ', BIGINT '1'),
    (CHAR 'F', CHAR 'M', CHAR 'Secondary           ', BIGINT '1', INTEGER '9000', BIGINT '1', CHAR 'Good      ', BIGINT '1'),
    (CHAR 'F', CHAR 'M', CHAR 'Unknown             ', BIGINT '1', INTEGER '8500', BIGINT '1', CHAR 'High Risk ', BIGINT '1'),
    (CHAR 'F', CHAR 'M', CHAR 'Unknown             ', BIGINT '1', INTEGER '10000', BIGINT '1', CHAR 'Good      ', BIGINT '1'),
    (CHAR 'F', CHAR 'S', CHAR '2 yr Degree         ', BIGINT '1', INTEGER '5000', BIGINT '1', CHAR 'Unknown   ', BIGINT '1'),
    (CHAR 'F', CHAR 'S', CHAR '2 yr Degree         ', BIGINT '1', INTEGER '10000', BIGINT '1', CHAR 'Low Risk  ', BIGINT '1'),
    (CHAR 'F', CHAR 'S', CHAR '4 yr Degree         ', BIGINT '1', INTEGER '5000', BIGINT '1', CHAR 'High Risk ', BIGINT '1'),
    (CHAR 'F', CHAR 'S', CHAR 'College             ', BIGINT '1', INTEGER '500', BIGINT '1', CHAR 'Low Risk  ', BIGINT '1'),
    (CHAR 'F', CHAR 'S', CHAR 'Secondary           ', BIGINT '1', INTEGER '4000', BIGINT '1', CHAR 'High Risk ', BIGINT '1'),
    (CHAR 'F', CHAR 'U', CHAR '2 yr Degree         ', BIGINT '1', INTEGER '7500', BIGINT '1', CHAR 'Low Risk  ', BIGINT '1'),
    (CHAR 'F', CHAR 'U', CHAR '4 yr Degree         ', BIGINT '1', INTEGER '500', BIGINT '1', CHAR 'High Risk ', BIGINT '1'),
    (CHAR 'F', CHAR 'U', CHAR 'Secondary           ', BIGINT '1', INTEGER '4000', BIGINT '1', CHAR 'Unknown   ', BIGINT '1'),
    (CHAR 'F', CHAR 'W', CHAR 'Advanced Degree     ', BIGINT '1', INTEGER '1000', BIGINT '1', CHAR 'High Risk ', BIGINT '1'),
    (CHAR 'F', CHAR 'W', CHAR 'College             ', BIGINT '1', INTEGER '2500', BIGINT '1', CHAR 'Unknown   ', BIGINT '1'),
    (CHAR 'F', CHAR 'W', CHAR 'College             ', BIGINT '1', INTEGER '5500', BIGINT '1', CHAR 'High Risk ', BIGINT '1'),
    (CHAR 'F', CHAR 'W', CHAR 'Secondary           ', BIGINT '1', INTEGER '3500', BIGINT '1', CHAR 'High Risk ', BIGINT '1'),
    (CHAR 'F', CHAR 'W', CHAR 'Secondary           ', BIGINT '1', INTEGER '4000', BIGINT '1', CHAR 'Low Risk  ', BIGINT '1'),
    (CHAR 'M', CHAR 'D', CHAR 'Primary             ', BIGINT '1', INTEGER '1500', BIGINT '1', CHAR 'Unknown   ', BIGINT '1'),
    (CHAR 'M', CHAR 'M', CHAR '2 yr Degree         ', BIGINT '1', INTEGER '7000', BIGINT '1', CHAR 'High Risk ', BIGINT '1'),
    (CHAR 'M', CHAR 'M', CHAR 'College             ', BIGINT '1', INTEGER '7500', BIGINT '1', CHAR 'Good      ', BIGINT '1'),
    (CHAR 'M', CHAR 'M', CHAR 'Unknown             ', BIGINT '1', INTEGER '7500', BIGINT '1', CHAR 'High Risk ', BIGINT '1'),
    (CHAR 'M', CHAR 'S', CHAR 'Secondary           ', BIGINT '1', INTEGER '6500', BIGINT '1', CHAR 'High Risk ', BIGINT '1'),
    (CHAR 'M', CHAR 'U', CHAR 'Advanced Degree     ', BIGINT '1', INTEGER '8500', BIGINT '1', CHAR 'Unknown   ', BIGINT '1'),
    (CHAR 'M', CHAR 'U', CHAR 'College             ', BIGINT '1', INTEGER '5500', BIGINT '1', CHAR 'High Risk ', BIGINT '1')
)
