::-------------------------------------------------------------
::
:: Modifications Copyright 2019 Graz University of Technology
::
:: Licensed to the Apache Software Foundation (ASF) under one
:: or more contributor license agreements.  See the NOTICE file
:: distributed with this work for additional information
:: regarding copyright ownership.  The ASF licenses this file
:: to you under the Apache License, Version 2.0 (the
:: "License"); you may not use this file except in compliance
:: with the License.  You may obtain a copy of the License at
:: 
::   http://www.apache.org/licenses/LICENSE-2.0
:: 
:: Unless required by applicable law or agreed to in writing,
:: software distributed under the License is distributed on an
:: "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
:: KIND, either express or implied.  See the License for the
:: specific language governing permissions and limitations
:: under the License.
::
::-------------------------------------------------------------

@ECHO OFF

IF "%~1" == ""  GOTO Err
IF "%~1" == "-help" GOTO Msg
IF "%~1" == "-h" GOTO Msg

setLocal EnableDelayedExpansion

SET HADOOP_HOME=%CD%/lib/hadoop

set CLASSPATH=./lib/*

set LOG4JPROP=log4j.properties

for /f "tokens=1,* delims= " %%a in ("%*") do set ALLBUTFIRST=%%b

IF "%SYSTEMDS_STANDALONE_OPTS%" == "" (
  SET SYSTEMDS_STANDALONE_OPTS=-Xmx4g -Xms4g -Xmn400m
)

:: construct the java command with options and arguments
set CMD=java %SYSTEMDS_STANDALONE_OPTS% ^
     -cp %CLASSPATH% ^
     -Dlog4j.configuration=file:%LOG4JPROP% ^
     org.tugraz.sysds.api.DMLScript ^
     -f %1 ^
     -exec singlenode ^
     -config SystemDS-config.xml ^
     %ALLBUTFIRST%

:: execute the java command
%CMD%

:: if there was an error, display the full java command
::IF  ERRORLEVEL 1 (
::  ECHO Failed to run SystemDS. Exit code: %ERRORLEVEL%
::  SET LF=^
::
::
::  :: keep empty lines above for the line breaks
::  ECHO %CMD:      =!LF!     %
::  EXIT /B %ERRORLEVEL%
::)

GOTO End

:Err
ECHO Wrong Usage. Please provide DML filename to be executed.
GOTO Msg

:Msg
ECHO Usage: runStandaloneSystemDS.bat ^<dml-filename^> [arguments] [-help]
ECHO Default Java options (-Xmx4g -Xms4g -Xmn400m) can be overridden by setting SYSTEMDS_STANDALONE_OPTS.
ECHO Script internally invokes 'java [SYSTEMDS_STANDALONE_OPTS] -cp ./lib/* -Dlog4j.configuration=file:log4j.properties org.tugraz.sysds.api.DMLScript -f ^<dml-filename^> -exec singlenode -config SystemDS-config.xml [arguments]'

:End
