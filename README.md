# Lakehouse Solution using AWS Glue, S3, Python, and Spark

## Project Overview
This project builds a lakehouse solution on AWS to support STEDI data scientists. The solution involves using AWS Glue, S3, Python, and Spark to process and sanitize data from various sources, ensuring that only relevant data is available for analysis.

## Workflow Summary
1. **Data Ingestion**:
   - Create S3 directories for `customer_landing`, `step_trainer_landing`, and `accelerometer_landing` zones.
   - Copy initial data into these S3 directories.
   - DDLs for the tables below
      https://github.com/cocobrice/Stedi_Human_Balance_Analytics/blob/main/SQL%20Scripts/accelerometer_landing.sql
      https://github.com/cocobrice/Stedi_Human_Balance_Analytics/blob/main/SQL%20Scripts/customer_landing.sql
      https://github.com/cocobrice/Stedi_Human_Balance_Analytics/blob/main/SQL%20Scripts/step_trainer_landing.sql

2. **Initial Data Exploration**:
   - Create three Glue tables for `customer_landing`, 'step_trainer_landing' and `accelerometer_landing`.
   - Query these tables using Athena and capture screenshots of the results.
     https://github.com/cocobrice/Stedi_Human_Balance_Analytics/blob/main/Screenshots/customer_landing_count.pdf
     https://github.com/cocobrice/Stedi_Human_Balance_Analytics/blob/main/Screenshots/accelrometer_landing_count.pdf
     https://github.com/cocobrice/Stedi_Human_Balance_Analytics/blob/main/Screenshots/customer_landing_share_null_count.pdf
     https://github.com/cocobrice/Stedi_Human_Balance_Analytics/blob/main/Screenshots/step_trainer_landing_count.pdf
     
3. **Data Sanitization**:
   - Create AWS Glue Jobs to sanitize customer and accelerometer data:
     - **Customer Data**: Store only records of customers who agreed to share their data in a `customer_trusted` table.
     - **Accelerometer Data**: Store only readings from customers who agreed to share their data in an `accelerometer_trusted` table.
        https://github.com/cocobrice/Stedi_Human_Balance_Analytics/blob/main/Glue%20Scripts/accelerometer_to_trusted.py
        https://github.com/cocobrice/Stedi_Human_Balance_Analytics/blob/main/Glue%20Scripts/customer_landing_to_trusted.py

   - Verify these tables by querying them in Athena and capturing screenshots.
     https://github.com/cocobrice/Stedi_Human_Balance_Analytics/blob/main/Screenshots/customer_trusted_count.pdf
     https://github.com/cocobrice/Stedi_Human_Balance_Analytics/blob/main/Screenshots/customer_trusted_null_count.pdf
     https://github.com/cocobrice/Stedi_Human_Balance_Analytics/blob/main/Screenshots/accelerometer_count.pdf

4. **Data Quality Issue Resolution**:
   - Due to a defect with serial numbers in the fulfillment data, write a Glue job to sanitize and curate the customer data:
     - Create a `customers_curated` table with only customers who have corresponding accelerometer data and agreed to share their data.
      https://github.com/cocobrice/Stedi_Human_Balance_Analytics/blob/main/Glue%20Scripts/Customer_to_curated.py
      https://github.com/cocobrice/Stedi_Human_Balance_Analytics/blob/main/Screenshots/customer_curated_count.pdf

5. **Advanced Data Processing**:
   - Create two Glue Studio jobs:
     - **Step Trainer Data**: Populate a `step_trainer_trusted` table with Step Trainer records for customers with accelerometer data.
     - **Aggregated Data**: Create an aggregated table `machine_learning_curated` that includes Step Trainer and corresponding accelerometer readings for the same timestamp, for customers who agreed to share their data.
      https://github.com/cocobrice/Stedi_Human_Balance_Analytics/blob/main/Glue%20Scripts/step_trainer_trusted.py
      https://github.com/cocobrice/Stedi_Human_Balance_Analytics/blob/main/Glue%20Scripts/machine_learning_curated.py

      https://github.com/cocobrice/Stedi_Human_Balance_Analytics/blob/main/Screenshots/step_trainer_trusted_count.pdf
      https://github.com/cocobrice/Stedi_Human_Balance_Analytics/blob/main/Screenshots/machine_learning_curated_count.pdf



## Tables and Expected Row Counts
- **Landing Zone**:
  - Customer: 956
  - Accelerometer: 81273
  - Step Trainer: 28680
- **Trusted Zone**:
  - Customer: 482
  - Accelerometer: 40981
  - Step Trainer: 14460
- **Curated Zone**:
  - Customer: 482
  - Machine Learning: 43681

## Guidelines
- Utilize Transform - SQL Query nodes for data transformations to avoid unexpected results.
- Verify row counts at each stage to ensure data integrity.

## License
This project is licensed under the MIT License.
