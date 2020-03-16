#!/bin/bash
#-------------------------------------------------------------
#
# Modifications Copyright 2020 Graz University of Technology
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#-------------------------------------------------------------


##############################################################
# This script is part of the SystemDS binary release. It is
# meant to work out of the box when unzipping the
# systemds-<version>.zip (or tbz) file.
#
# Make configuration changes here:
##############################################################

# run locally by default
DISTRIBUTED=0

# specify paramteters  to java when running locally here
SYSTEMDS_STANDALONE_OPTS="\
      -Xmx4g\
      -Xms4g\
      -Xmn400m"

# specify parameters to pass to spark-submit when running on spark here
SYSTEMDS_DISTRIBUTED_OPTS="\
      --master yarn \
      --deploy-mode client\
      --driver-memory 96g \
      --num-executors 4 \
      --executor-memory 64g \
      --executor-cores 16"

##############################################################
# No need to touch the content below. These commands launch
# SystemDS based on the settings above.
##############################################################

# changes the separator in a string (eg to convert from / directory separator to \

change_delimiter()
{
  output=$( "IFS=$2 echo $1" )
  echo "$output"
}

change_delimiter $(pwd) ":"
exit 0

# error help print
printUsageExit()
{
cat << EOF
Usage: $0 <dml-filename> [arguments] [-help] [-dist]
    arguments - The arguments specified after the DML script are passed to SystemDS.
                Specify parameters that need to go to java/spark-submit by editing this
                run script.
    -help     - Print this usage message and exit
    -dist     - Run distributed (invokes spark-submit). Default is to run locally (invokes java)

Set custom launch configuration by editing SYSTEMDS_STANDALONE_OPTS and/or SYSTEMDS_DISTRIBUTED_OPTS
EOF
  exit 1
}

# option parsing
options=$(getopt -l "help,dist" -a -o "hd" -- "$@")
while options; do
  case $options in
    d )
      DISTRIBUTED=1
      ;;
    h ) echo Warning: Help requested. Will exit after usage message;
        printUsageExit
        ;;
    \? ) echo Warning: Help requested. Will exit after usage message;
        printUsageExit
        ;;
    * ) echo Error: Unexpected error while processing options;
  esac
  shift
done

# print an error if no dml file is supplied
if [ -z $1 ] ; then
    echo "Wrong Usage.";
    printUsageExit;
fi

# Peel off first argument so that $@ contains arguments to DML script
SCRIPT_FILE=$1
shift

FIND_JAR_RESULT
SYSTEMDS_JAR_FILE=
find -iname systemds-?.?.?.jar

# detect operating system to set correct directory separator
if [ "$OSTYPE" == "win32" ] ||  [ "$OSTYPE" == "msys" ] ; then
  DIR_SEP=\\
else
  DIR_SEP=/
fi

# set java class path
CLASSPATH=".${DIR_SEP}lib${DIR_SEP}*"

# check if log4j config file exists, otherwise unset
# to run with a non fatal complaint by SystemDS
LOG4JPROP="conf${DIR_SEP}log4j.properties"
if [ ! -f $LOG4JPROP ]; then
  LOG4JPROP=""
fi

# same as above: set config file param if the
# file exists
CONFIG_FILE="conf${DIR_SEP}SystemDS-config.xml"
if [ ! -f $CONFIG_FILE ]; then
  CONFIG_FILE=""
fi

#build the command to run
if [ -z $DISTRIBUTED ]; then
  CMD=" \
  java ${SYSTEMDS_STANDALONE_OPTS} \
  -cp ${CLASSPATH} \
  -Dlog4j.configuration=file:${LOG4JPROP} \
  org.tugraz.sysds.api.DMLScript \
  -f ${SCRIPT_FILE} \
  -exec singlenode \
  -config $CONFIG_FILE \
  $*"
else
  export SPARK_MAJOR_VERSION=2

  CMD=" \
  spark-submit ${SYSTEMDS_DISTRIBUTED_OPTS} \
  $SYSTEMDS_JAR_FILE \
  $*"
fi

# run
$CMD
