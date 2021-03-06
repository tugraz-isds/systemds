<!--
{% comment %}
Modifications Copyright 2020 Graz University of Technology

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% end comment %}
-->

## Scripts to run SystemDS
This directory contains scripts to launch systemds.   

## Setting SYSTEMDS_ROOT environment variable
In order to run SystemDS from your development directory and leave the 
SystemDS source tree untouched, the following setup could be used (example for bash):
 ```shell script
$ export SYSTEMDS_ROOT=/home/$USER/systemds
$ export PATH=$SYSTEMDS_ROOT/bin:$PATH
```

## Running a first example:
To see SystemDS in action a simple example using the `Univar-stats.dml` 
script can be executed. This example is taken from the 
[SystemML documentation](http://apache.github.io/systemml/standalone-guide). 
The relevant commands to run this example with SystemDS will be listed here.
See their documentation for further details.  

#### Example preparations
```shell script
# download test data
$ wget -P data/ http://archive.ics.uci.edu/ml/machine-learning-databases/haberman/haberman.data

# generate a metadata file for the dataset
$ echo '{"rows": 306, "cols": 4, "format": "csv"}' > data/haberman.data.mtd

# generate type description for the data
$ echo '1,1,1,2' > data/types.csv
$ echo '{"rows": 1, "cols": 4, "format": "csv"}' > data/types.csv.mtd
```
#### Executing the DML script
```shell script
$ bin/systemds.sh Univar-Stats.dml -nvargs X=data/haberman.data TYPES=data/types.csv STATS=data/univarOut.mtx CONSOLE_OUTPUT=TRUE
```

#### Using Intel MKL native instructions
To use the MKL acceleration download and install the latest MKL library from [1], 
set the environment variables with the MKL-provided script `$ compilervars.sh intel64` and set 
the option `sysds.native.blas` in `SystemDS-config.xml`.

## Further reading 

More documentation is available in the [docs directory of our github repository](https://github.com/tugraz-isds/systemds/tree/master/docs) 
