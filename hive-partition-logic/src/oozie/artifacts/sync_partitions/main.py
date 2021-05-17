import datetime

from datetime import datetime
from datetime import datetime, date, timedelta
import argparse
from modules.ProcessLogger import setup_logging
from modules.ReadConfigObj import ReadConfigObj

import glob 
from modules.AddPartitionsDeltas import GenerateALterAddPartitions
from modules.ExecuteHiveStatements import ExecuteHqls
from modules.QueryMetastore import QueryMetastore
import os
import importlib

from  modules.envvalues import day,tdy,BETA_LOG_HDFS,PROD_LOG_HDFS, BETA_SECRETS,PROD_SECRETS, REGION,BETA_CHECKPOINT_HDFS,PROD_CHECKPOINT_HDFS,BETA_HQL_HDFS,PROD_HQL_HDFS
from modules.GetMetaStoreSecrets import GetHiveSecrets
import json
from modules.GetAlerts import GetAlerts
import boto3
import traceback
import subprocess


logger = setup_logging().getLogger(__name__)    
alertObj = GetAlerts()

def string_buffer_to_file(filename, string_buffer_object):
    '''
    Flushes string buffer object to file
    '''
    try:
        with open(filename, 'w') as writ_obj:
            writ_obj.write(string_buffer_object)
    except IOError as ioerr:
        raise ioerr
        logger.error("{}".format(ioerr))


def restore_last_checkpoint():
    '''
    Parses checkpoint file and returns back info
    '''
    try:
        last_checkpoint = glob.glob('./checkpoint/.-*-RUNNING')
        return last_checkpoint
    except Exception as err:
        raise err
        logger.error("{}".format(err))


def remove_processed_database_files(file_path):
    '''
    cleans processed database files after the processing
    '''
    try:
        os.remove(file_path)
        logger.info("Remove the processed database file {}".format(file_path))
    except IOError as err:
        raise err
        logger.error("{}".format(err))


def get_add_alters(database, prev_db_val, subset_result,config_header_read,checkpoint_val,env_checkpoint_path):
    '''
    Calls daily or hourly partition modules as per config info provided

    Dynamically imports the module (uses import lib) and 
    creates instances of the class & method calls for daily or hourly partitions

    Parameters:
    database (string): database name
    prev_db_val (string): previously iterated database
    subset_result (list): list of tables in a database to generate alters for.
    config_header_read (dict): config info to be parsed for partition formats, module, class and method names
    checkpoint_val (string): date info , helps in backfill in case of failures

    Return:
    db_alter_statements (string): alter statements for a particular database 
    filename (string): .hql file_name to store the alter statements generated 
    checkpoint_file (string): checkpoint fail name , used in case of failures

    '''
    logger.info("Current and Previous Database Values {0} {1}".format(database, prev_db_val))
    filename =   "./" + prev_db_val + ".hql"
    checkpoint_file = "./.-{}-RUNNING".format(prev_db_val)


    with open(checkpoint_file, 'w') as checkpoint:
        checkpoint.write("Running")
        logger.info("Check Point File Created for database at location {}".format(checkpoint_file))
    put_chk_pnt_to_hdfs = "hdfs dfs -put ./.-{0}-RUNNING {1}".format(prev_db_val,env_checkpoint_path)
    logger.debug("put checkpoint file command {}".format(put_chk_pnt_to_hdfs))
    os.system(put_chk_pnt_to_hdfs)

    # Reading module_name, class_name and method name to be invoked from config_header_read 
    module_name,class_name = (config_header_read['class_instance'].rsplit('.',1))
    method_name = config_header_read['method_instance']

    # Invoking module and creating instance for the class
    module = importlib.import_module(module_name)
    class_ = getattr(module, class_name)
    instance = class_()

    # Extracting partition from "partition format object" in config_header_read
    partition_frmt_dict = config_header_read['partition_format']

    # Building argument list to call the method
    kargs_list = {"partition_format":partition_frmt_dict,"subsetresult":subset_result,"checkpoint_val":checkpoint_val}
    
    return_list_alter_date_hour = getattr(instance,method_name)(**kargs_list)
    db_alter_statements = return_list_alter_date_hour[0]

    if len(return_list_alter_date_hour) == 2 :
        date_hour_store_fl =   "./" + prev_db_val + "_checkpoint_fl"
        string_buffer_to_file(date_hour_store_fl, return_list_alter_date_hour[1])
    put_chk_pnt_val_to_hdfs = "hdfs dfs -put {0} {1}".format(date_hour_store_fl,env_checkpoint_path)
    logger.debug("put checkpoint value file command {}".format(put_chk_pnt_val_to_hdfs))
    os.system(put_chk_pnt_val_to_hdfs)
    
    logger.info("Alter Statements Generated for Database {}".format(prev_db_val))
    
    return db_alter_statements, filename, checkpoint_file



def subset_generator(num_of_rows, rows, generate_alter_add_obj, config_header_read,checkpoint_val,env_checkpoint_path):
    '''
    Generates subset for each database from multiple databases extracted from metaquery

    Parameters:
    num_of_rows (int): Total rows extracted from metaquery execution
    rows (string): All rows extracted from total
    generate_alter_add_obj (object): Add
    '''

    logger.debug("In Subset Generator Method {0},{1},{2}".format(num_of_rows,config_header_read,checkpoint_val))
    logger.debug("In Subset Generator Method rows {}".format(rows))
    row_counter = 0
    prev_db_val = None
    subset_result = []
    for row_idx in range(0, num_of_rows):
        row_tuple = tuple(map(lambda x: x.strip(), rows[row_idx].split('\t')))
        database = generate_alter_add_obj.desearlize_row(row_tuple)[0]
        logger.debug("In Subset Generator Method - Database Name {}".format(database))
        logger.debug("In Subset Generator Method - Tuple {}".format(row_tuple))
        if prev_db_val is None:
            prev_db_val = database
        if prev_db_val != database:
            db_alter_statements, filename, checkpoint_file = get_add_alters(database, prev_db_val,subset_result,config_header_read,\
                                                                                checkpoint_val,env_checkpoint_path)
            yield db_alter_statements, filename, checkpoint_file
            prev_db_val = database
            subset_result = []
        subset_result.append(row_tuple)
        if row_counter == num_of_rows - 1:
            db_alter_statements, filename, checkpoint_file = get_add_alters(database, prev_db_val, \
                                                                            subset_result,config_header_read,checkpoint_val,env_checkpoint_path)
            yield db_alter_statements, filename, checkpoint_file
        row_counter += 1
        







if __name__=='__main__':
    '''
    Main Call - does below functionality
    - Parsing Arguments sent  
    - Parsing Config.ini
    - Calls to Secret manager, Metastore query
    - Initiating classes, methods and instances for Alter Statements creation
    '''

    logger.info("In Main Function Call..")

    # Argument parsers takes in conf_header, environment, workflow and custom checkpoint value as arguements
    parser = argparse.ArgumentParser(description= \
                                         "This script generates alter add partition deltas" \
                                         "and execute them over the beeline")
    parser.add_argument("--conf_header", "-c", required=True, \
                        help="Specify the Header section that you want read from config")
    parser.add_argument("--env", "-e", required=True,choices=['BETA','PROD'], \
                        help="Specify the env where the deployment will run")

    parser.add_argument("--workflow", "-wf", required=True, \
                        help="Specify the oozie workflow name")

    parser.add_argument("--chkpoint_val_sub", "-ch", required=False, \
                        help="Specify the checkpoint value substitute." \
                        "This will override or eliminate the need of checkpoint file"\
                        "for backfilling the alter statements")
    args = parser.parse_args()

    # header_section needs to be as defined in config.ini ( ex: partition_daily, partition_hourly etc)
    header_section = args.conf_header
    logger.info("Partition arguments passed to the script conf_header: {0}, workflow:{1}, environment:{2}".format(header_section,args.workflow,args.env))

    # Assigning Alert class variable workflow with user given workflow_name
    GetAlerts.workflow = args.workflow
    GetAlerts.env = args.env

    # Reading config.ini , will be downloaded into current working directory in Oozie
    config_path = "./config.ini"
    metastore_conf_obj = ReadConfigObj(config_path)
    config = metastore_conf_obj.read_config()

    # Extract Info from config based on --conf_header provided by User Arguments
    config_header_read = metastore_conf_obj.get_query_properties(header_section)
    
    # Create secret name based on Environment
    secret_name = eval(args.env+"_"+"SECRETS")
    
    # Create Secrets Object and fetch the response from Secrets
    get_hive_secrets = GetHiveSecrets(secret_name,REGION)
    logger.debug("secret manager response body {}".format(str(get_hive_secrets.get_secret()))) #get_hive_secrets.get_secret()
    
    # If Extracting Metastore connection info fails
    # Raise Exception and Alert
    try:
        secrets = json.loads(get_hive_secrets.get_secret()['SecretString'])
        query_metastore = QueryMetastore(config_header_read['metastore_query_param'],args.env)
        query_metastore.host = str(secrets["host"])
        query_metastore.user = str(secrets["username"])
        query_metastore.password = str(secrets["password"])
        query_metastore.port = str(secrets["port"])
    except TypeError as typerr:
        cause_msg = "Cause: TypeError"
        logger.error("Unsupported type exception {}".format(typerr))
        alertObj.sns_alert(traceback.format_exc(),cause_msg,typerr,__name__,alertObj.workflow)
        raise typerr

    logger.debug("host,port {0},{1}".format(secrets["host"],secrets["port"]))
    
    # Run Metastore query and fetch result_set of the databases and tables to process
    result_set = query_metastore.get_metastore_result().strip()
    logger.debug("result set {}".format(result_set))
    
    # Create Checkpoint directory path and HQL path based on Environment
    env_checkpoint_path = args.env+"_CHECKPOINT_HDFS"
    env_checkpoint_path = eval(env_checkpoint_path).format(args.workflow)

    
    env_hql_path = args.env+"_HQL_HDFS"
    env_hql_path = eval(env_hql_path).format(args.workflow)
    
    logger.debug("checkpoint file path {}".format(env_checkpoint_path))
    logger.debug("hql file path {}".format(env_hql_path))
    
    # Check if directory exists ?
    filexistchk="hdfs dfs -test -d "+env_checkpoint_path+";echo $?"
    logger.debug("file exist check command {}".format(filexistchk))
    filexistchk_output=subprocess.Popen(filexistchk,shell=True,stdout=subprocess.PIPE).communicate()
    
    # By default , it does not exist
    checkpoint_bool = False

    # If directory exists, there is a failure on previous run
    # So Set checkpoint_bool = True for backfill process to commence
    if '1' not in str(filexistchk_output[0]):
        logger.debug('The given env_checkpoint_path dir exists : {}'.format(env_checkpoint_path))
        checkpoint_bool = True
    
    """
    result_set =  "'db_name\ttab_name\tmax_partition\nagentplatform_raw\tagent_1\tdatekey=2020-12-16/hour=18\n \
    agentplatform_raw\tagent_2\tNULL \
    \nagentplatform_raw\tagent_3\tdatekey=2020-12-16/hour=18\nplatform_1\tplatform_tab_1\tdatekey=2020-12-16/hour=18\n \
    platform_1\tplatform_tab_2\tdatekey=2020-12-16/hour=18\nreltio_raw\treltio_response_raw\tNULL'"
    checkpoint_bool = False
    env_checkpoint_path = args.env+"_CHECKPOINT_HDFS"
    env_checkpoint_path = eval(env_checkpoint_path).format(args.workflow)
    logger.debug("checkpoint file path {}".format(env_checkpoint_path))
    
    result_set =  "db_name\ttab_name\tmax_partition\nagentplatform_raw\tagent_1\tdatekey=2020-12-16\n \
    agentplatform_raw\tagent_2\tNULL \
    \nagentplatform_raw\tagent_3\tdatekey=2020-12-16\nplatform_1\tplatform_tab_1\tdatekey=2020-12-16\n \
    platform_1\tplatform_tab_2\tdatekey=2020-12-16\nreltio_raw\treltio_response_raw\tNULL"
    """

    # If result_set from Metastore is not empty, Proceed!
    if result_set != "":

        # If Custom Checkpoint Value is not provided by user - "chkpoint_val_sub" - is None
        if args.chkpoint_val_sub is None:
            
            # If Checkpoint_bool is True - GET checkpoint files from HDFS 
            if(checkpoint_bool == True):
                hdfs_checkpoint_get = "hdfs dfs -get {0} {1}".format(env_checkpoint_path,".")
                logger.debug("checkpoint get command {}".format(hdfs_checkpoint_get))
                os.system(hdfs_checkpoint_get)

            # Look for .-db-RUNNING file in checkpoint directory
            last_checkpoint_database = restore_last_checkpoint()

            # If Checkpoint file exists
            if len(last_checkpoint_database) == 1:

                # Extract database value from .-database-RUNNING file
                logger.debug("check point exists, running for backfill data")
                database = last_checkpoint_database[0].split('/')[-1].split('-')[1]

                # Extract Checkpoint value from database_checkpoint_fl
                fl_read = open("./checkpoint/"+database + "_checkpoint_fl", "r")
                checkpoint_val= fl_read.read().strip()
                logger.info("checkpoint val found : {}".format(checkpoint_val))

                # Filter result set with checkpoint database val
                result_set = result_set[result_set.index(database):].split('\n')

            # If Checkpoint file does not exists
            else:
                logger.debug("check point does not exist, running for current day")
                checkpoint_val = ""
                result_set = result_set.split('\n')[1:]

        # If Custom Checkpoint Value is provided by User through Argument
        else:
            logger.info("Check Point Value given through Args {}".format(args.chkpoint_val_sub))
            checkpoint_val = args.chkpoint_val_sub
            result_set = result_set.split('\n')[1:]
            logger.debug("trimmed result set {}".format(result_set))

        # Validate User passed argument args.chkpoint_val_sub matches the format provided in config file
        # If not, Raise Alert and Exception 
        try:
            if (checkpoint_val!=""):
                datetime.strptime(checkpoint_val,config_header_read['partition_format']["format"])
                logger.info("Check Point Value in correct format{0} {1}".format(checkpoint_val,config_header_read['partition_format']["format"]))
        except ValueError as valerr:
            cause_msg = "Cause: Unable to Parse : Date Format Error "
            logger.error("Date Format error {}".format(str(valerr)))
            alertObj.sns_alert(traceback.format_exc(),cause_msg,valerr,__name__,args.workflow)
            raise valerr

        # Hive Execution Object Creation
        ExecuteHqls_obj = ExecuteHqls(args.env)
        logger.debug("Hive JDBC URL FOR ENV {0} - {1}".format(args.env,ExecuteHqls_obj.get_env_val()))
        
        # Below loop generates alter statements for individual databases provided
        rows = result_set
        generate_alter_add_obj = GenerateALterAddPartitions()
        for alters, db_hql_file, checkpoint_file in subset_generator(len(rows), rows, generate_alter_add_obj,
                                                                 config_header_read,checkpoint_val,env_checkpoint_path):
            if alters != "":
                
                # Create empty hql file on local fs
                string_buffer_to_file(db_hql_file, alters)
                logger.info("Alter Statements saved to the database file")

                #Hive Exec Alter statements
                ExecuteHqls_obj.run_beeline(db_hql_file)

                # Clean up checkpoint file from local file system
                remove_processed_database_files(checkpoint_file)
                
                # Put hql files into HDFS location - Debugging Purpose Only**
                
                put_hql_to_hdfs = "hdfs dfs -put -f {0} {1}".format(db_hql_file,env_hql_path)
                logger.debug("hql put command {}".format(put_hql_to_hdfs))
                os.system(put_hql_to_hdfs)

                # Clean up hql file from local file system
                remove_processed_database_files(db_hql_file)
                
                # Remove HDFS Checkpoint Directory After Successfully executing hql file
                rm_checkpoint = "hdfs dfs -rm -r {}".format(env_checkpoint_path)
                logger.debug("remove check point directory command {}".format(rm_checkpoint))
                os.system(rm_checkpoint)
                
        # Creating Log Path on HDFS
        env_log_path = args.env+"_LOG_HDFS"
        env_log_path = eval(env_log_path).format(args.workflow)


        

        # Check if directory exists ?
        log_dir_check = "hdfs dfs -test -d "+env_log_path+";echo $?"
        logger.debug("log directory check command {}".format(log_dir_check))
        log_dir_check_output=subprocess.Popen(log_dir_check,shell=True,stdout=subprocess.PIPE).communicate()


        # If directory exists, there is a failure on previous run
        # So Set checkpoint_bool = True for backfill process to commence
        if '1' in str(log_dir_check_output[0]):
            logger.debug('The given env_log_path dir does not exist : {}'.format(env_log_path))
            log_dir_create = "hdfs dfs -mkdir -p {}".format(env_log_path)
            os.system(log_dir_create)

        # Truncating Log Files on 1st of every month
        if day == 1:
            rm_logs = "hdfs dfs -rmr {}*".format(env_log_path)
            os.system(rm_logs)
            
        # Put the Log files on HDFS Workflow location
        put_log_to_hdfs = "hdfs dfs -put ./Hive_Sync_Partition_Error_{0}.log {1}".format(tdy,env_log_path)
        logger.info("HDFS Put Log {}".format(put_log_to_hdfs))
        os.system(put_log_to_hdfs)

    #print(timeit.timeit())
    
    
        
 
