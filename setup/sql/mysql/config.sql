-- Grant the necessary permissions to the Fivetran user
GRANT EXECUTE ON PROCEDURE mysql.rds_kill to 'fivetran'@'%';
GRANT SELECT ON *.* TO fivetran@'%';

-- Update the permissions for the master user
GRANT SESSION_VARIABLES_ADMIN ON *.* TO admin;

-- Select the desired target database
USE master;

-- Create a table for the policy records
CREATE TABLE IF NOT EXISTS policies (
    CUST_ID INT,
    POLICY_NO VARCHAR(128) PRIMARY KEY,
    POLICYTYPE VARCHAR(128),
    POL_ISSUE_DATE VARCHAR(128),
    POL_EFF_DATE VARCHAR(128),
    POL_EXPIRY_DATE VARCHAR(128),
    BODY VARCHAR(64),
    MAKE VARCHAR(128),
    MODEL VARCHAR(256),
    MODEL_YEAR FLOAT,
    CHASSIS_NO VARCHAR(512),
    USE_OF_VEHICLE VARCHAR(128),
    DRV_DOB VARCHAR(128),
    BOROUGH VARCHAR(256),
    NEIGHBORHOOD VARCHAR(512),
    ZIP_CODE INT,
    PRODUCT VARCHAR(128),
    SUM_INSURED FLOAT,
    PREMIUM FLOAT,
    DEDUCTABLE FLOAT
);

-- Load the sample policy records into the allocated table
LOAD DATA LOCAL INFILE '../data/samples/mysql/policies.csv'
  INTO TABLE policies
  FIELDS TERMINATED BY ','
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  IGNORE 1 ROWS;