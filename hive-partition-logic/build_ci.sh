#!/usr/bin/env bash
#!/bin/bash
# set -eo pipefail
echo "Build Process Initiated for Hive Sync Partitions......."
env=$1
echo "Build for Environment ""$env"

ls -lrt
if [ $# -eq 2 ]; then
   circle_ci_url=$2
else
  circle_ci_url="null"
fi



mkdir -p publish
mkdir -p .bin

copy_configs() {

   if [ $# -ne 1 ];then
     echo "copy Configs Function takes exactly one argument, git diff result, please check the git diff result"
     exit 1
   fi

   local diff_result="$1" 
   for obj in $(echo "$diff_result" | awk '{print $2}')
     do
      echo "Git Diff Objects....."
      echo "actual path ""$obj"
      src_obj=$(echo "$obj" | cut -d "/" -f 1-1)
      echo "source obj ""$src_obj"
      object=$(echo "$obj" | sed 's/src\///')
      echo "$object"
      object_dir=$(echo "$object" | cut -d "/" -f 1-3)
      echo "$object_dir"
      mkdir build
      mkdir -p publish/"$env"/"$object_dir"
      ls -lrt
      pwd
      echo "copying all the modules required to build directory....."
      cp -R "$src_obj"/"$object_dir"/modules ./build
      cp -R "$src_obj"/"$object_dir"/resources ./build
      cp "$src_obj"/"$object_dir"/main.py ./build
      cp -R "$src_obj"/"$object_dir"/test ./build
      cd ./build
      

      echo "Zipping all python modules"

      zip -r9 hive_sync_daily_partition.zip .
      echo "Copy Zip file to publish directory"
      cp hive_sync_daily_partition.zip ../publish/"$env"/"$object_dir"
      cd ../publish/"$env"/"$object_dir"
      pwd
      ls -lrt
      exit 0
     done
};

if [ $# -eq 1 ]; then
    diff_result=$(git diff-tree --no-commit-id --name-status -r HEAD | grep "src")
    echo "diff_result"
    echo $diff_result
    copy_configs "$diff_result"
else
    echo "else executed"
    diff_result=$(git diff-tree --no-commit-id --name-status -r `echo ${circle_ci_url} | cut -d/ -f 7` | grep "deployment-config")
    copy_configs "$diff_result"
 fi
 

    
    
