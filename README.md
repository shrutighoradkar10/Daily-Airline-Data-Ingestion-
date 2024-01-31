# Daily-Airline-Data-Ingestion-
Orchestrating daily data ingestion with AWS step function.

## Overview
This project automates the daily ingestion of airline data from CSV files stored in an S3 bucket. The entire process is orchestrated seamlessly, leveraging AWS services for a robust and efficient data pipeline.

Key AWS Services:

1. CloudTrail & EventBridge:
   CloudTrail captures S3 PutObject-level API activity, providing detailed insights into file interactions.
   EventBridge is set up with rules that precisely match CloudTrail events related to S3 object-level API activities. This ensures that the pipeline is triggered only when relevant     events occur.

2. Step Functions:


   ![Execution Workflow of Step Function](https://github.com/shrutighoradkar10/Daily-Airline-Data-Ingestion-/assets/75423631/7619ff4f-21cf-40e2-8357-0ebbc2a6471f)


   The Step Function orchestrates the data pipeline by initiating a daily data crawler in AWS Glue for the uploaded CSV files in S3. 

   It then checks the status of the crawler and waits until it completes. Once the crawler has finished, the flow continues to start the Glue job. The Step Function is configured    to wait for the task to complete, ensuring a seamless transition.

   After the Glue job execution, the Step Function checks the status of the job.
   If the job is successful, an SNS  sends an email notification, confirming the successful completion of the Glue job. In case of a failure, the Step Function triggers another 
   SNS notification, indicating that the Glue job has failed.

4. AWS Glue Job:


   ![Glue Job](https://github.com/shrutighoradkar10/Daily-Airline-Data-Ingestion-/assets/75423631/e5ecd100-9e10-422a-9539-b4e730605f47)


   The Step Function initiates an AWS Glue job, responsible for extracting, transforming, and loading (ETL) data from CSV files.
   Glue provides a scalable and serverless ETL solution, optimizing data transformation processes.

5. Amazon Redshift:

    The Glue job uploads the processed data into a Redshift table for deriving meaningful business insights.

## Advantages:

    Automation: The entire workflow is automated, minimizing manual intervention.
    
    Scalability: Utilizes serverless components for scalability and cost-effectiveness.
    
    Maintainability: Clearly defined steps and AWS services enhance project maintainability.
    
    Analytical Power: Leverages Redshift for efficient storage and querying of airline data.
