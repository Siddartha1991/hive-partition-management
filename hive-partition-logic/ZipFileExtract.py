import boto3

import argparse


if __name__=='__main__':
    print("in ZipFileExtract.py script")

    # Argument parsers takes in conf_header and env as arguements
    parser = argparse.ArgumentParser(description= \
                                         "This script downloads code package, config and metaquery")
    parser.add_argument("--bucket_name", "-b", required=True, \
                    help="Specify Bucket Name to download the artifacts from")
    parser.add_argument("--artifact_uri", "-a", required=True, \
                        help="Specify Artifact URI to download Code Package Zip")
    parser.add_argument("--config_uri", "-c", required=True, \
                        help="Specify Config URI to download config.ini")

    parser.add_argument("--metaquery_uri", "-m", required=True, \
                        help="Specify metaquery_uri to download metaquery_uri.sql")
    args = parser.parse_args()
    print("in the s3 download script")
    try:
        s3 = boto3.client('s3')
    except Exception as e:
        print("boto3 Client Exception")
        raise e
    print(args.bucket_name , args.artifact_uri , args.artifact_uri.rsplit("/",1)[1])
    print(args.artifact_uri)
    print(args.config_uri)
    print(args.metaquery_uri)
    try:
        s3.download_file(args.bucket_name,args.artifact_uri , args.artifact_uri.rsplit("/",1)[1])
        s3.download_file(args.bucket_name,args.config_uri , args.config_uri.rsplit("/",1)[1])
        s3.download_file(args.bucket_name,args.metaquery_uri , args.metaquery_uri.rsplit("/",1)[1])
    except Exception as e:
        print("s3 download exception")
        raise e
    print("s3 download complete")