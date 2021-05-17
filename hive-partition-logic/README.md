Hive_Partition
The scripts are featured to create alter queries for hive partirioning



How to use this:

To use this framework, please make to do the following steps:

Step 1: Update the Config.ini file


1.	The Config.ini file is in this repo: 

https://git.rockfin.com/DI-PlatformExperience/hive_sync_parttitions_config/tree/branch_beta/resources/oozie/artifacts/Alter_partitions_hourly/v1


2.	Create your file structure in this repo. It should follow this file structure: resources/oozie/artifacts/(oozie_workflow_name) /v1/config.ini

Here oozie_workflow_name refers to the name of the OOZIE job for your use case.


3.	Update the following parameters under these required Headers in the config.ini 

![Screen Shot 2021-01-12 at 9 12 59 PM](https://git.rockfin.com/storage/user/2086/files/66121c00-55b0-11eb-9369-c1f9140d0de4)

Note: Do Not change anything for the header: ql_di_datalake

The main parameters that should be changed in this Config.ini file is:


|Header| Variable | Type | Description | Required |
| :-------:|----------|----------|---------|---------|
|Metastore_query_param(both partition_hourly and partition_format)|Regex_filter|String|This filter is to get all the ‘datekey’ partition results from meta store |Yes, should not be changed|
| |database_filter |comma separated values |This filter will give results of the tables in that dbs| Yes|
| |Table_filter |comma separated values|	|This filter will restrict the results for given tables only|	No|
|Partition_format(for partition_hourly) |datekey|string|Format of the partition keys It can also have nested partitions(Ex:datekey= ‘%Y-%m-%d’ (or) year=’%Y’ month=’%m’ day= ‘%d’|Yes|
| |hour|string| Format of the hourly partitions Ex: hour=‘%H’| Yes|
| |format|string|‘%Y-%M-%D %H’|	Yes|
|Partition_format(for partition_daily)|datekey|string|Format of the partition keys It can also have nested partitions (Ex:datekey= ‘%Y-%m-%d’ (or) year=’%Y’ month=’%m’ day= ‘%d’|Yes|
| |format|string|‘%Y-%M-%D’|Yes|

By passing these parameters in config.ini, we will generate a dynamic sql query to get results from the metastore. So, make sure you update all the required details

Once you update all the above details, Circle Ci pipeline will find the latest changes and moves them to the utils bucket in out Dataaccount bucket



Step 2: Update the metaquery.sql

1.	metaquery.sql is found under this repo:

https://git.rockfin.com/DIPlatformExperience/hive_sync_partition_metaquery/tree/branch_beta/resources/oozie/artifacts/metastore_query/v1

2.	the only use case where the user might need to create a new metaquery.sql file is when they want to update the filters in the query:

![Screen Shot 2021-01-12 at 9 48 52 PM](https://git.rockfin.com/storage/user/2086/files/52b48000-55b3-11eb-8e3a-b83ec3e5c99a)        

Create your file structure in this repo. It should follow this file structure: resources/oozie/artifacts/metastore_query /v2/metaquery.sql


Once you update all the above details, Circle Ci pipeline will find the latest changes and moves them to the utils bucket in out Dataaccount bucket





Step 3: The main Source code for this framework can be found under this repo

https://git.rockfin.com/DI-PlatformExperience/hive-partition-logic

User need not to modify or zip the code base, but this will be handled by the Circle Ci pipeline and copies the zip file to the utils bucket and code will be downloaded directly.





Step 4: Creating a OOZIE workflow

1.	Create an oozie workflow. You can follow this page to create a workflow: https://confluence/display/DtL/Oozie
2.	After creating a folder in HDFS with your Workflow name:  
      a. copy the deploy.sh file into that folder  
      b. copy ZipFileExtract.py file into that folder  
      c. create two folders logs and sql  
      d. logs - will contain any errors during runs  
      e. sql - will contain Alter statements processed during most recent run  

3.	Add all the below Environment Variables to the OOZIE workflow

      Variable | Description | Required | Default |
     | :-------:|----------|----------|----------|
     |bucket |Name of the Utils bucket|	Yes| ql-datalake-utils-710269499330-us-east-2-nonprod|
     |artifact_prefix |Name of the oozie workflow that user created|Yes|sync_partitions|
     |workflow_name |Name of the oozie workflow|Yes|[oozie_workflow_name]|
     |artifact_version |Artifact Version|Yes|v13|
     |base_prefix |	|	Yes|oozie|
     |artifact_name |zip file name of Src code| Yes|hive_sync_daily_partition.zip|
     |environment	|Name of the Environment where the code supposed to run Ex: PROD/BETA| Yes|BETA
     |config_version |Version of config.ini file| Yes|[config_version]|
     |metaquery_version |Version of Metaquery| Yes|v3|
     
     
     
 Step 5: Modify the deploy.sh file

 In the Deploy.sh file, Do the following

1.	Uncomment the Python submit command as per your use case.

•	If your use case is to update partitions_hourly, then uncomment this:
python3 main.py --conf_header partition_hourly --env BETA -wf Alter_partitions_hourly


•	If your use case is to update partitions_daily, then uncomment this:

python3 main.py --conf_header partition_hourly --env BETA -wf Alter_partitions_hourly


2.	Add the following arguments for the python submit:

      Variable | Description | Required |
     | :-------:|----------|----------|
     |conf_header |Specify the Header section that you want to read from config|Yes|
     |env |Specify the env where the deployment will run|Yes|
     |workflow |Specify the oozie workflow name|Yes|
     |chkpoint_val_sub |This will override or eliminate the need of checkpoint file for backfilling the alter            statements|Optional|



















