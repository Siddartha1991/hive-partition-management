'''
GetAlerts Version 1:
This script generates AWS SNS Alerts for any Errors occuring during Sync Partitions Execution
'''
import boto3
from modules.ProcessLogger import setup_logging
from modules.envvalues import BETA_SNS_TOPIC,PROD_SNS_TOPIC,REGION,BETA_HQL_HDFS

logger = setup_logging().getLogger(__name__)

class GetAlerts():
    workflow= ""
    env = ""
    def __init__(self):
        '''
        Sets subject for the Email Alert to be sent
        '''
        self.subject = "Alert Notification Email for Workflow : " 

    def sns_alert(self,trace_string,cause_msg,exception,module_name,workflow_name):
        '''
        This Method creates Body for the email alert to be sent
        Creates boto3 client and publishes SNS alert to the topic "hourly_partition_alerting"
        '''
        alert_body = "{} \n\n\nException Found : {} in Module '{}' \n \n \n {}".format(cause_msg,str(type(exception)),module_name,trace_string)
        sns_topic=eval(self.env+"_SNS_TOPIC")
        logger.info("GetAlerts Module SNS topic provided: {}".format(sns_topic))
        client = boto3.client('sns',region_name=REGION)
        response = client.publish( \
            TopicArn=eval(self.env+"_SNS_TOPIC"),\
            Message=alert_body,\
            Subject=self.subject + workflow_name,\
            MessageAttributes={
                        'workflow_name': {
                            'DataType': 'String',
                            'StringValue': workflow_name
                        }
                    }\
        )

        
        logger.info("SNS Alert Published on topic - hourly_partition_alerting")
        
        