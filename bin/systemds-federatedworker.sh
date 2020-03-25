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

# if not set by env, set to 1 to disable setup output of this script
if [ -z "$FEDERATED_SYSDS_QUIET" ]; then
  FEDERATED_SYSDS_QUIET=0
fi


if [ -n "$FEDERATED_SYSTEMDS_STANDALONE_OPTS" ]; then
	echo "Overriding SYSTEMDS_STANDALONE_OPTS with env var: $FEDERATED_SYSTEMDS_STANDALONE_OPTS"
else
  # specify paramteters  to java when running locally here
  SYSTEMDS_STANDALONE_OPTS="\
      -Xmx4g\
      -Xms4g\
      -Xmn400m"
fi


##############################################################
# No need to touch the content below. These commands launch
# the federated worker node of SystemDS based on the settings above.
##############################################################


#-------------------------------------------------------------
# some helper functions

printUsageExit()
{
cat << EOF
Usage: $0 [SystemDS.jar] [portnumber]

    SystemDS.jar - Specify a custom SystemDS.jar file (this will be prepended to the classpath
    port         - The port to open for the federated worker.
	arguments    - The arguments specified after the DML script are passed to SystemDS.
                   Specify parameters that need to go to java/spark-submit by editing this
                   run script.
    -help        - Print this usage message and exit

Set custom launch configuration by setting/editing FEDERATED_SYSTEMDS_STANDALONE_OPTS

Set FEDERATED_SYSDS_QUIET=1 to omit extra information printed by this run script.
EOF
  exit 1
}


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

# an echo toggle
print_out()
{
  if [ $FEDERATED_SYSDS_QUIET == 0 ]; then
    echo "$1"
  fi
}

# converts a relative path to an absolute path
rel_path()
{
  declare -n ret=${1}
	source=$(pwd)
	target=${2}
	common_part=$source
#	echo "${target#$common_part}"
	back=
	while [ "${target#$common_part}" == "${target}" ]; do
	  common_part=$(dirname "$common_part")
	  back="../${back}"
	  # echo ${back}
	  # echo ${common_part}
	  # if [ back == common_part ]; then 
		# break 
	  # fi
	done

#	 echo ${back}${target#$common_part/}

if [ -z "${back=}" ]; then
  ret=./${target#$common_part/}
  else
	ret=${back}${target#$common_part/}
	fi
}

# converts an absolute to a relative path
abs_path()
{
	OLD=$(pwd)
	cd "$1"
	ABS=$(pwd)
	cd "$OLD"
	echo "$ABS"/
}

# above be helper functions
#-------------------------------------------------------------

if [ -z "$1" ]; then
	PORT=""
else
	PORT="$1"
fi


if [ -z "$SYSTEMDS_ROOT" ] ; then
  SYSTEMDS_ROOT=.
else
  # construct a relative path
	rel_path REL "$(abs_path ${SYSTEMDS_ROOT})"
	SYSTEMDS_ROOT=${REL}
	echo "Using existing SystemDS at ${SYSTEMDS_ROOT}"
fi;

# find me a SystemDS jar file to run
if [ -z "$SYSTEMDS_JAR_FILE" ];then
  SYSTEMDS_JAR_FILE=$(find "$SYSTEMDS_ROOT" -iname "systemds.jar" | tail -n 1)
  if [ -z "$SYSTEMDS_JAR_FILE" ];then
    SYSTEMDS_JAR_FILE=$(find "$SYSTEMDS_ROOT" -iname "systemds-?.?.?.jar" | tail -n 1)
    if [ -z "$SYSTEMDS_JAR_FILE" ];then
      SYSTEMDS_JAR_FILE=$(find "$SYSTEMDS_ROOT" -iname "systemds-?.?.?-SNAPSHOT.jar" | tail -n 1)
    fi
  fi
else
	echo "Using user supplied systemds jar file $SYSTEMDS_JAR_FILE"
fi

# check if log4j config file exists, otherwise unset
# to run with a non fatal complaint by SystemDS
LOG4JPROP=$(find "$SYSTEMDS_ROOT" -iname "log4j*properties" | tail -n 1)
if [ -z "${LOG4JPROP}" ]; then
  LOG4JPROP=""
fi

# same as above: set config file param if the file exists
CONFIG_FILE=$(find "$SYSTEMDS_ROOT" -iname "SystemDS*config*.xml" | tail -n 1)
if [ -z "$CONFIG_FILE" ]; then
  CONFIG_FILE=""  
else
  CONFIG_FILE="--config $CONFIG_FILE"
fi

DIR_SEP=/
# detect operating system to set correct path separator
if [ "$OSTYPE" == "win32" ] ||  [ "$OSTYPE" == "msys" ] ; then
  PATH_SEP=\;
else
  PATH_SEP=:
fi

# make the jar path relative to skip issues with Windows paths
JARNAME=$(basename "$SYSTEMDS_JAR_FILE")

# relative path to jar file
rel_path R "$(abs_path "$(dirname "$SYSTEMDS_JAR_FILE")")"
SYSTEMDS_JAR_FILE="${R}${DIR_SEP}${JARNAME}"

# find hadoop home
if [ -z "$HADOOP_HOME" ]; then
  HADOOP_HOME=$(abs_path "$(find "$SYSTEMDS_ROOT" -iname hadoop | tail -n 1 )")
  export HADOOP_HOME
fi

# add hadoop home to path and lib path for loading hadoop jni
rel_path HADOOP_REL "$HADOOP_HOME"
export PATH=${PATH}${PATH_SEP}${HADOOP_REL}bin
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}${PATH_SEP}${HADOOP_REL}bin

# set java class path
CLASSPATH="${SYSTEMDS_JAR_FILE}${PATH_SEP} \
          ${SYSTEMDS_ROOT}${DIR_SEP}lib${DIR_SEP}*${PATH_SEP} \
          ${SYSTEMDS_ROOT}${DIR_SEP}target${DIR_SEP}lib${DIR_SEP}*"

# trim whitespace (introduced by the line breaks above)
CLASSPATH=$(echo "${CLASSPATH}" | tr -d '[:space:]')

print_out "###############################################################################"
print_out "#  SYSTEMDS_ROOT= $SYSTEMDS_ROOT"
print_out "#  SYSTEMDS_JAR_FILE= $SYSTEMDS_JAR_FILE"
print_out "#  CONFIG_FILE= $CONFIG_FILE"
print_out "#  LOG4JPROP= $LOG4JPROP"
print_out "#  CLASSPATH= $CLASSPATH"
print_out "#  HADOOP_HOME= $HADOOP_HOME"
print_out "#"
print_out "#  starting Fedederated worker on port $PORT"
print_out "###############################################################################"

CMD=" \
  java $SYSTEMDS_STANDALONE_OPTS \
  -cp $CLASSPATH \
  -Dlog4j.configuration=file:$LOG4JPROP \
  org.tugraz.sysds.api.DMLScript \
  -w $PORT"

$CMD