#!/bin/bash
# assumptions:
#   * current directory of local file system of the master node 
#     contains this file (command-line script) and all other files of your project you have created 
#     (whole content of your project1.zip file registered as a solution of the project)
#   * the input directory located in the user's home directory in HDFS 
#     contains two subdirectories: datasource1 and datasource4 
#     with uzipped source dataset project files - dataset (1) and (4) respectively

echo " "
echo ">>>> removing leftovers from previous launches"
# delete the output directory for mapreduce job (3)
if $(hadoop fs -test -d ./output_mr3) ; then hadoop fs -rm -f -r ./output_mr3; fi
# delete the output directory for the final project result (6)
if $(hadoop fs -test -d ./output6) ; then hadoop fs -rm -f -r ./output6; fi
# delete the directory with the other project's files (scripts, jar files and everything that needs to be available in HDFS to launch this script)
if $(hadoop fs -test -d ./project_files) ; then hadoop fs -rm -f -r ./project_files; fi
# remove the local output directory containing the final result of the project (6)
if $(test -d ./output6) ; then rm -rf ./output6; fi


echo " "
echo ">>>> copying scripts, jar files and anything else that needs to be available in HDFS to launch this script"
hadoop fs -mkdir -p project_files
hadoop fs -copyFromLocal *.jar project_files
hadoop fs -copyFromLocal *.pig project_files

echo " "
echo ">>>> launching the MapReduce job - processing (2)"
hadoop jar Main.jar MapReduceMain input/datasource1 output_mr3

echo " "
echo ">>>> launching the Hive/Pig script - processing (5)"
pig -f script.pig

echo " "
echo ">>>> getting the final result (6) from HDFS to the local file system"
mkdir -p ./output6
hadoop fs -copyToLocal output6/* ./output6

echo " "
echo " "
echo " "
echo " "
echo ">>>> presenting the obtained the final result (6)"
cat ./output6/*