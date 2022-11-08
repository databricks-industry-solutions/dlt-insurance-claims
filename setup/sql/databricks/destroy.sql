-- Delete the MySQL source schema and all of the associated tables
DROP SCHEMA insurance_demo_mysql_master CASCADE;
DROP SCHEMA insurance_demo_mysql_sys CASCADE;

-- Delete the S3 source schema and all of the associated tables
DROP SCHEMA insurance_demo_s3 CASCADE;

-- Delete the MongoDB source schema and all of the associated tables
DROP SCHEMA insurance_demo_mongodb_master CASCADE;

-- Delete the Lakehouse schema and all of the associated tables
DROP SCHEMA insurance_demo_lakehouse CASCADE;