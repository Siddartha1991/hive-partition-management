#!/bin/bash


echo "Deployment started"

echo "copying zip"

kinit -kt /etc/security/keytabs/svc-oozie-wkflows-np.keytab svc-oozie-wkflows-np@MITST.CORPTST.ROCKFIN.COM 
#kinit -kt /etc/security/keytabs/svc-oozie-workflows.keytab svc-oozie-workflows


empty_found(){
echo "** Empty Argument Found $1 **" 
exit 1
}

echo "copy started"
bucket="${BUCKET}"
artifact_prefix="${ARTIFACT_PREFIX}"
workflow_name="${WORKFLOW_NAME}"
artifact_version="${ARTIFACT_VERSION}"
base_prefix="${BASE_PREFIX}"
artifact_name="${ARTIFACT_NAME}"
environment="${ENVIRONMENT}"
config_version="${CONFIG_VERSION}"
metaquery_version="${METAQUERY_VERSION}"

[ -z "$bucket" ] && empty_found BUCKET || echo $bucket
[ -z "$artifact_prefix" ] && empty_found ARTIFACT_PREFIX || echo $artifact_prefix
[ -z "$workflow_name" ] && empty_found WORKFLOW_NAME || echo $workflow_name
[ -z "$artifact_version" ] && empty_found ARTIFACT_VERSION || echo $artifact_version
[ -z "$base_prefix" ] && empty_found BASE_PREFIX || echo $base_prefix
[ -z "$artifact_name" ] && empty_found ARTIFACT_NAME || echo $artifact_name
[ -z "$environment" ] && empty_found ENVIRONMENT || echo $environment
[ -z "$config_version" ] && empty_found CONFIG_VERSION || echo $config_version
[ -z "$metaquery_version" ] && empty_found METAQUERY_VERSION || echo $metaquery_version

artifact_uri=$environment"/"$base_prefix"/artifacts/"$artifact_prefix"/"$artifact_version"/"$artifact_name
echo $artifact_uri


config_uri=$environment"/"$base_prefix"/artifacts/"$workflow_name"/"$config_version"/config.ini"
echo $config_uri


metaquery_uri=$environment"/"$base_prefix"/artifacts/metastore_query/"$metaquery_version"/metaquery.sql"
echo $metaquery_uri



echo "Running Zip File Extract"
/opt/anaconda3/envs/ds_env_v1.0/bin/python3 ZipFileExtract.py -b $bucket -a $artifact_uri -c $config_uri -m $metaquery_uri


echo "Unzipping the code packagae"
unzip $artifact_name

echo "Alter Add Partitions process started"
/opt/anaconda3/envs/ds_env_v1.0/bin/python3 main.py --conf_header partition_daily --env BETA -wf workflow_name -ch ""
