#!/usr/bin/env bash
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

# set to 1 to run spark-submit instead of local java
DISTRIBUTED=0

# set to 1 to disable setup output of this script
QUIET=0

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


# error help print
printUsageExit()
{
cat << EOF
Usage: $0 [SystemDS.jar] <dml-filename> [arguments] [-help]

    SystemDS.jar - Specify a custom SystemDS.jar file (this will be prepended to the classpath
                   or fed to spark-submit
    dml-filename - The script file to run
    arguments    - The arguments specified after the DML script are passed to SystemDS.
                   Specify parameters that need to go to java/spark-submit by editing this
                   run script.
    -help        - Print this usage message and exit

Set custom launch configuration by editing SYSTEMDS_STANDALONE_OPTS and/or SYSTEMDS_DISTRIBUTED_OPTS
EOF
  exit 1
}

# print an error if no dml file is supplied
if [ -z "$1" ] ; then
    echo "Wrong Usage.";
    printUsageExit;
fi

while getopts "h:" options; do
  case $options in
    h ) echo Warning: Help requested. Will exit after usage message;
        printUsageExit
        ;;
    \? ) echo Warning: Help requested. Will exit after usage message;
        printUsageExit
        ;;
    * ) echo Error: Unexpected error while processing options;
  esac
done

win_delim()
{
  declare -n ret=$1
  OLDIFS=$IFS
  IFS="/"
  new_path=""
  for i in ${ret}; do
    new_path="${new_path}$i\\"
  done
  IFS=$OLDIFS
  ret=${new_path:0:-1}
}

status_output()
{
  if [ $QUIET == 0 ]; then
    echo -e "$1"
  fi
}

# Peel off first and/or second argument so that $@ contains arguments to DML script
if  grep -q "$1" <<< "jar"; then
  SYSTEMDS_JAR_FILE=$1
  shift
  SCRIPT_FILE=$2
  shift
else
  SCRIPT_FILE=$1
  shift
fi


if [ -z "$SYSTEMDS_ROOT" ] ; then
  SYSTEMDS_ROOT=.
else
  echo "Using existing SystemDS at ${SYSTEMDS_ROOT}"
fi;


# find me a SystemDS jar file to run
if [ -z "${SYSTEMDS_JAR_FILE}" ];then
  SYSTEMDS_JAR_FILE=$(find $SYSTEMDS_ROOT -iname "systemds.jar" | tail -n 1)
  if [ -z "${SYSTEMDS_JAR_FILE}" ];then
    SYSTEMDS_JAR_FILE=$(find $SYSTEMDS_ROOT -iname "systemds-?.?.?.jar" | tail -n 1)
  fi
fi

# check if log4j config file exists, otherwise unset
# to run with a non fatal complaint by SystemDS
#LOG4JPROP="conf${DIR_SEP}log4j.properties"
#if [ ! -f "$LOG4JPROP" ]; then
LOG4JPROP=$(find $SYSTEMDS_ROOT -iname "log4j*properties" | tail -n 1)
if [ -z "${LOG4JPROP}" ]; then
  LOG4JPROP=""
fi

# same as above: set config file param if the
# file exists
#CONFIG_FILE="conf${DIR_SEP}SystemDS-config.xml"
#if [ ! -f $CONFIG_FILE ]; then
CONFIG_FILE=$(find $SYSTEMDS_ROOT -iname "SystemDS*config*.xml" | tail -n 1)
if [ -z "${CONFIG_FILE}" ]; then
  CONFIG_FILE=""
else
  CONFIG_FILE="--config ${CONFIG_FILE}"
fi

# detect operating system to set correct directory separator
if [ "$OSTYPE" == "win32" ] ||  [ "$OSTYPE" == "msys" ] ; then
  DIR_SEP=\\
  CLASSPATH_SEP=;
  # fix separator in find output on windows
  win_delim SYSTEMDS_JAR_FILE
  if [ -n "$LOG4JPROP" ]; then win_delim LOG4JPROP; fi
  if [ -n "$CONFIG_FILE" ]; then win_delim CONFIG_FILE;  fi
else
  DIR_SEP=/
  CLASSPATH_SEP=:
fi

# set java class path
CLASSPATH="${SYSTEMDS_JAR_FILE}${CLASSPATH_SEP}\
          ${SYSTEMDS_ROOT}${DIR_SEP}lib${DIR_SEP}*${CLASSPATH_SEP}\
          ${SYSTEMDS_ROOT}${DIR_SEP}target${DIR_SEP}lib${DIR_SEP}*"
# trim whitespace (introduced by the line breaks above)
CLASSPATH=$(echo -e "$CLASSPATH" | tr -d '[:space:]')

status_output "###############################################################################"
status_output "#  SYSTEMDS_ROOT= ${SYSTEMDS_ROOT}"
status_output "#  SYSTEMDS_JAR_FILE= ${SYSTEMDS_JAR_FILE}"
status_output "#  CONFIG_FILE= ${CONFIG_FILE}"
status_output "#  LOG4JPROP= ${LOG4JPROP}"

#build the command to run
if [ ${DISTRIBUTED} == 0 ]; then
  status_output "#\n#  Runnign locally with opts: $*"
  status_output "###############################################################################"
  CMD=" \
  java ${SYSTEMDS_STANDALONE_OPTS} \
  -cp ${CLASSPATH} \
  -Dlog4j.configuration=file:${LOG4JPROP} \
  org.tugraz.sysds.api.DMLScript \
  -f ${SCRIPT_FILE} \
  -exec singlenode \
  $CONFIG_FILE \
  $*"
else
  status_output "#\n#  Running distributed with opts: $*"
  status_output "###############################################################################"

  export SPARK_MAJOR_VERSION=2
  CMD=" \
  spark-submit ${SYSTEMDS_DISTRIBUTED_OPTS} \
  $SYSTEMDS_JAR_FILE \
  $*"
fi

# run
$CMD
