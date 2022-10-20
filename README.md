# [Airflow-EMR-Spark](https://github.com/vaishali-yasala/Airflow-EMR-Spark/blob/main/dags/airflow_spark_emr_job.py)
Using airflow upload data to s3 bucket and then create emr cluster, read data into hdfs from s3 as a step, submit a job as a step , wait for the step to finish and then terminate the emr cluster

## How it works

### Step One: Move data and Spark scripts to S3 Bucket
Initially, we create a DAG file using airflow, which uploads files from local to AWS S3 chosen bucket. These files include:
1. [sales-data.csv](https://raw.githubusercontent.com/vaishali-yasala/Airflow-EMR-Spark/main/dags/data/sales-data.csv) - this contains the data we are trying to analyze 
2. [main.py](https://github.com/vaishali-yasala/Airflow-EMR-Spark/blob/main/dags/scripts/main.py) - The script below is written in Python to analyze the sales data using Spark RDD transformations and actions.

![Script](Images/Script.png)
Moving data 
![DAG](Images/Moving_to_S3.png)

### Step Two: Create an EMR cluster
Then, airflow creates an EMR cluster using <i>EmrCreateJobFlowOperator</i> operator. It creates a cluster and stores the EMR cluster id (unique identifier) in <i>  xcom </i>, which is a key value store used to access variables across Airflow tasks.Below we see the configurations defined to help in creating an EMR cluster.

![EMR](Images/JobFlow_configuration.png)


- EMR_EC2_DefaultRole is the default managed role that is assigned to our EC2 instances as an instance profile when we create our cluster. The role has a default managed policy called "AmazonElasticMapReduceforEC2Role" which has a few risky actions.

- EMR_DefaultRole. Allows Amazon EMR to call other AWS services on your behalf when provisioning resources and performing service-level actions. This role is required for all clusters. 

### Step Three: Add steps
Add multiple steps to the EMR cluster created. These include:
1. <b> Move data from S3 to HDFS </b> - to copy sales data which was uploaded earlier to S3 bucket to EMR cluster HDFS. 
2. <b> Give total sales amount of each country</b> - This step runs the job with Spark application as step type on main.py file. It gives the total sales price from each country from the sales data. Output is stored in HDFS. 
3. <b> Move output data from HDFS to S3 </b> - this sends the output from HDFS to S3 chosen bucket folder output_data.

![Spark_steps](Images/Spark_steps.png)
![Steps](Images/Steps.png)

### Step Four: Terminate the EMR cluster
Once the three steps are successfully completed, airflow terminates the EMR cluster created earlier. 
![Steps](Images/Steps.png)

## Result

The DAG below depicts the flow of Airflow, EMR and Spark association to process required data dynamically. The green outline indicates the success of this DAG.

![DAG](Images/Airflow_DAG.png)


Let us look at the output folder where the results files are stored in S3. part-00000 to part-00009 represents data from 10 partitions of data. 
![S3_Output](Images/S3_Output_Folder.png) 

This is the screenshot of data stored in partition 1.
![output_part_1](Images/Output%231.png)