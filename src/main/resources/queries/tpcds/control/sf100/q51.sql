SELECT *
FROM
(
  VALUES
    (BIGINT '2', DATE '2000-01-01', DECIMAL '52.08', DECIMAL '14.68', DECIMAL '52.08', DECIMAL '14.68'),
    (BIGINT '2', DATE '2000-01-04', DECIMAL '211.07', NULL, DECIMAL '211.07', DECIMAL '180.48'),
    (BIGINT '2', DATE '2000-01-05', NULL, DECIMAL '197.78', DECIMAL '211.07', DECIMAL '197.78'),
    (BIGINT '2', DATE '2000-01-06', DECIMAL '273.23', DECIMAL '270.41', DECIMAL '273.23', DECIMAL '270.41'),
    (BIGINT '7', DATE '2000-01-02', DECIMAL '196.48', DECIMAL '169.74', DECIMAL '196.48', DECIMAL '169.74'),
    (BIGINT '7', DATE '2000-01-03', NULL, DECIMAL '192.56', DECIMAL '196.48', DECIMAL '192.56'),
    (BIGINT '13', DATE '2000-01-26', DECIMAL '626.52', DECIMAL '599.41', DECIMAL '626.52', DECIMAL '599.41'),
    (BIGINT '13', DATE '2000-01-28', DECIMAL '727.66', NULL, DECIMAL '727.66', DECIMAL '711.08'),
    (BIGINT '13', DATE '2000-01-31', DECIMAL '768.05', DECIMAL '760.30', DECIMAL '768.05', DECIMAL '760.30'),
    (BIGINT '13', DATE '2000-02-02', DECIMAL '897.51', DECIMAL '761.41', DECIMAL '897.51', DECIMAL '761.41'),
    (BIGINT '13', DATE '2000-02-03', NULL, DECIMAL '793.66', DECIMAL '897.51', DECIMAL '793.66'),
    (BIGINT '13', DATE '2000-02-04', NULL, DECIMAL '830.52', DECIMAL '897.51', DECIMAL '830.52'),
    (BIGINT '13', DATE '2000-02-05', NULL, DECIMAL '855.63', DECIMAL '897.51', DECIMAL '855.63'),
    (BIGINT '13', DATE '2000-02-07', DECIMAL '898.55', DECIMAL '876.75', DECIMAL '898.55', DECIMAL '876.75'),
    (BIGINT '13', DATE '2000-02-10', DECIMAL '926.21', NULL, DECIMAL '926.21', DECIMAL '876.75'),
    (BIGINT '17', DATE '2000-01-02', DECIMAL '139.90', DECIMAL '56.24', DECIMAL '139.90', DECIMAL '56.24'),
    (BIGINT '17', DATE '2000-01-03', NULL, DECIMAL '77.22', DECIMAL '139.90', DECIMAL '77.22'),
    (BIGINT '20', DATE '2000-01-16', DECIMAL '514.18', NULL, DECIMAL '514.18', DECIMAL '402.96'),
    (BIGINT '20', DATE '2000-01-17', DECIMAL '554.05', NULL, DECIMAL '554.05', DECIMAL '402.96'),
    (BIGINT '20', DATE '2000-01-18', NULL, DECIMAL '496.25', DECIMAL '554.05', DECIMAL '496.25'),
    (BIGINT '20', DATE '2000-01-19', NULL, DECIMAL '540.32', DECIMAL '554.05', DECIMAL '540.32'),
    (BIGINT '23', DATE '2000-01-03', NULL, DECIMAL '8.13', DECIMAL '8.55', DECIMAL '8.13'),
    (BIGINT '23', DATE '2000-01-04', DECIMAL '172.25', DECIMAL '26.22', DECIMAL '172.25', DECIMAL '26.22'),
    (BIGINT '23', DATE '2000-01-07', DECIMAL '245.86', DECIMAL '109.88', DECIMAL '245.86', DECIMAL '109.88'),
    (BIGINT '23', DATE '2000-01-08', DECIMAL '248.32', DECIMAL '116.22', DECIMAL '248.32', DECIMAL '116.22'),
    (BIGINT '23', DATE '2000-01-09', NULL, DECIMAL '178.97', DECIMAL '248.32', DECIMAL '178.97'),
    (BIGINT '23', DATE '2000-01-12', NULL, DECIMAL '181.46', DECIMAL '248.32', DECIMAL '181.46'),
    (BIGINT '23', DATE '2000-01-13', DECIMAL '372.14', DECIMAL '248.71', DECIMAL '372.14', DECIMAL '248.71'),
    (BIGINT '23', DATE '2000-01-15', NULL, DECIMAL '329.93', DECIMAL '372.14', DECIMAL '329.93'),
    (BIGINT '23', DATE '2000-01-17', DECIMAL '395.13', NULL, DECIMAL '395.13', DECIMAL '329.93'),
    (BIGINT '23', DATE '2000-01-19', DECIMAL '442.14', DECIMAL '358.71', DECIMAL '442.14', DECIMAL '358.71'),
    (BIGINT '23', DATE '2000-01-23', NULL, DECIMAL '439.85', DECIMAL '442.14', DECIMAL '439.85'),
    (BIGINT '23', DATE '2000-01-25', DECIMAL '456.75', NULL, DECIMAL '456.75', DECIMAL '439.85'),
    (BIGINT '31', DATE '2000-01-02', DECIMAL '147.34', DECIMAL '137.76', DECIMAL '147.34', DECIMAL '137.76'),
    (BIGINT '31', DATE '2000-01-05', DECIMAL '248.55', DECIMAL '146.69', DECIMAL '248.55', DECIMAL '146.69'),
    (BIGINT '31', DATE '2000-01-06', DECIMAL '358.95', DECIMAL '281.90', DECIMAL '358.95', DECIMAL '281.90'),
    (BIGINT '31', DATE '2000-01-07', NULL, DECIMAL '282.62', DECIMAL '358.95', DECIMAL '282.62'),
    (BIGINT '31', DATE '2000-01-08', DECIMAL '379.13', DECIMAL '299.06', DECIMAL '379.13', DECIMAL '299.06'),
    (BIGINT '31', DATE '2000-01-09', DECIMAL '459.01', NULL, DECIMAL '459.01', DECIMAL '299.06'),
    (BIGINT '31', DATE '2000-01-10', DECIMAL '478.45', DECIMAL '299.11', DECIMAL '478.45', DECIMAL '299.11'),
    (BIGINT '31', DATE '2000-01-12', NULL, DECIMAL '322.16', DECIMAL '478.45', DECIMAL '322.16'),
    (BIGINT '31', DATE '2000-01-13', NULL, DECIMAL '336.06', DECIMAL '478.45', DECIMAL '336.06'),
    (BIGINT '31', DATE '2000-01-14', NULL, DECIMAL '348.26', DECIMAL '478.45', DECIMAL '348.26'),
    (BIGINT '31', DATE '2000-01-17', DECIMAL '621.83', NULL, DECIMAL '621.83', DECIMAL '348.26'),
    (BIGINT '31', DATE '2000-01-18', NULL, DECIMAL '373.51', DECIMAL '621.83', DECIMAL '373.51'),
    (BIGINT '31', DATE '2000-01-19', NULL, DECIMAL '381.12', DECIMAL '621.83', DECIMAL '381.12'),
    (BIGINT '31', DATE '2000-01-20', NULL, DECIMAL '444.79', DECIMAL '621.83', DECIMAL '444.79'),
    (BIGINT '31', DATE '2000-01-21', NULL, DECIMAL '454.41', DECIMAL '621.83', DECIMAL '454.41'),
    (BIGINT '31', DATE '2000-01-22', NULL, DECIMAL '499.69', DECIMAL '621.83', DECIMAL '499.69'),
    (BIGINT '31', DATE '2000-01-23', DECIMAL '669.50', NULL, DECIMAL '669.50', DECIMAL '499.69'),
    (BIGINT '31', DATE '2000-01-25', DECIMAL '722.25', NULL, DECIMAL '722.25', DECIMAL '499.69'),
    (BIGINT '31', DATE '2000-01-28', DECIMAL '834.17', DECIMAL '526.16', DECIMAL '834.17', DECIMAL '526.16'),
    (BIGINT '31', DATE '2000-01-29', DECIMAL '882.47', NULL, DECIMAL '882.47', DECIMAL '526.16'),
    (BIGINT '31', DATE '2000-01-30', NULL, DECIMAL '554.97', DECIMAL '882.47', DECIMAL '554.97'),
    (BIGINT '31', DATE '2000-02-01', NULL, DECIMAL '762.01', DECIMAL '882.47', DECIMAL '762.01'),
    (BIGINT '31', DATE '2000-02-02', NULL, DECIMAL '805.33', DECIMAL '882.47', DECIMAL '805.33'),
    (BIGINT '31', DATE '2000-02-03', DECIMAL '931.93', DECIMAL '855.00', DECIMAL '931.93', DECIMAL '855.00'),
    (BIGINT '32', DATE '2000-01-02', DECIMAL '295.20', DECIMAL '148.30', DECIMAL '295.20', DECIMAL '148.30'),
    (BIGINT '32', DATE '2000-01-03', NULL, DECIMAL '167.66', DECIMAL '295.20', DECIMAL '167.66'),
    (BIGINT '32', DATE '2000-01-04', DECIMAL '312.28', DECIMAL '200.91', DECIMAL '312.28', DECIMAL '200.91'),
    (BIGINT '32', DATE '2000-01-05', NULL, DECIMAL '206.31', DECIMAL '312.28', DECIMAL '206.31'),
    (BIGINT '32', DATE '2000-01-08', DECIMAL '373.35', NULL, DECIMAL '373.35', DECIMAL '206.31'),
    (BIGINT '32', DATE '2000-01-10', DECIMAL '419.12', NULL, DECIMAL '419.12', DECIMAL '206.31'),
    (BIGINT '32', DATE '2000-01-11', NULL, DECIMAL '250.84', DECIMAL '419.12', DECIMAL '250.84'),
    (BIGINT '32', DATE '2000-01-12', DECIMAL '431.06', DECIMAL '325.68', DECIMAL '431.06', DECIMAL '325.68'),
    (BIGINT '32', DATE '2000-01-13', NULL, DECIMAL '327.40', DECIMAL '431.06', DECIMAL '327.40'),
    (BIGINT '32', DATE '2000-01-14', NULL, DECIMAL '358.11', DECIMAL '431.06', DECIMAL '358.11'),
    (BIGINT '32', DATE '2000-01-15', DECIMAL '496.02', DECIMAL '381.27', DECIMAL '496.02', DECIMAL '381.27'),
    (BIGINT '32', DATE '2000-01-16', DECIMAL '537.30', DECIMAL '432.82', DECIMAL '537.30', DECIMAL '432.82'),
    (BIGINT '32', DATE '2000-01-17', NULL, DECIMAL '435.12', DECIMAL '537.30', DECIMAL '435.12'),
    (BIGINT '32', DATE '2000-01-18', NULL, DECIMAL '448.03', DECIMAL '537.30', DECIMAL '448.03'),
    (BIGINT '32', DATE '2000-02-03', DECIMAL '857.82', DECIMAL '849.16', DECIMAL '857.82', DECIMAL '849.16'),
    (BIGINT '32', DATE '2000-02-05', DECIMAL '893.16', NULL, DECIMAL '893.16', DECIMAL '863.89'),
    (BIGINT '34', DATE '2000-01-01', DECIMAL '170.57', DECIMAL '54.44', DECIMAL '170.57', DECIMAL '54.44'),
    (BIGINT '35', DATE '2000-01-02', DECIMAL '215.65', DECIMAL '69.12', DECIMAL '215.65', DECIMAL '69.12'),
    (BIGINT '35', DATE '2000-01-03', DECIMAL '268.15', NULL, DECIMAL '268.15', DECIMAL '69.12'),
    (BIGINT '35', DATE '2000-01-04', NULL, DECIMAL '167.59', DECIMAL '268.15', DECIMAL '167.59'),
    (BIGINT '35', DATE '2000-01-07', NULL, DECIMAL '177.22', DECIMAL '268.15', DECIMAL '177.22'),
    (BIGINT '35', DATE '2000-01-08', NULL, DECIMAL '192.00', DECIMAL '268.15', DECIMAL '192.00'),
    (BIGINT '35', DATE '2000-01-17', NULL, DECIMAL '217.07', DECIMAL '268.15', DECIMAL '217.07'),
    (BIGINT '35', DATE '2000-01-18', DECIMAL '271.26', NULL, DECIMAL '271.26', DECIMAL '217.07'),
    (BIGINT '35', DATE '2000-01-19', DECIMAL '439.64', NULL, DECIMAL '439.64', DECIMAL '217.07'),
    (BIGINT '35', DATE '2000-01-20', DECIMAL '482.91', DECIMAL '442.69', DECIMAL '482.91', DECIMAL '442.69'),
    (BIGINT '37', DATE '2000-01-02', DECIMAL '201.16', DECIMAL '181.52', DECIMAL '201.16', DECIMAL '181.52'),
    (BIGINT '37', DATE '2000-01-06', DECIMAL '365.28', NULL, DECIMAL '365.28', DECIMAL '349.15'),
    (BIGINT '37', DATE '2000-01-09', DECIMAL '432.15', DECIMAL '360.59', DECIMAL '432.15', DECIMAL '360.59'),
    (BIGINT '37', DATE '2000-01-11', NULL, DECIMAL '383.95', DECIMAL '432.15', DECIMAL '383.95'),
    (BIGINT '37', DATE '2000-01-12', DECIMAL '445.49', NULL, DECIMAL '445.49', DECIMAL '383.95'),
    (BIGINT '37', DATE '2000-01-13', DECIMAL '525.22', NULL, DECIMAL '525.22', DECIMAL '383.95'),
    (BIGINT '37', DATE '2000-01-14', DECIMAL '614.63', DECIMAL '412.79', DECIMAL '614.63', DECIMAL '412.79'),
    (BIGINT '37', DATE '2000-01-15', NULL, DECIMAL '472.67', DECIMAL '614.63', DECIMAL '472.67'),
    (BIGINT '37', DATE '2000-01-19', NULL, DECIMAL '541.49', DECIMAL '614.63', DECIMAL '541.49'),
    (BIGINT '43', DATE '2000-01-10', DECIMAL '408.64', NULL, DECIMAL '408.64', DECIMAL '369.98'),
    (BIGINT '43', DATE '2000-01-11', NULL, DECIMAL '376.30', DECIMAL '408.64', DECIMAL '376.30'),
    (BIGINT '43', DATE '2000-01-15', DECIMAL '496.57', NULL, DECIMAL '496.57', DECIMAL '448.82'),
    (BIGINT '47', DATE '2000-01-02', DECIMAL '80.58', DECIMAL '13.81', DECIMAL '80.58', DECIMAL '13.81'),
    (BIGINT '49', DATE '2000-01-01', DECIMAL '230.20', DECIMAL '65.02', DECIMAL '230.20', DECIMAL '65.02'),
    (BIGINT '65', DATE '2000-01-02', DECIMAL '126.57', DECIMAL '19.61', DECIMAL '126.57', DECIMAL '19.61'),
    (BIGINT '65', DATE '2000-01-03', NULL, DECIMAL '106.56', DECIMAL '126.57', DECIMAL '106.56'),
    (BIGINT '65', DATE '2000-01-04', NULL, DECIMAL '117.69', DECIMAL '126.57', DECIMAL '117.69')
)
