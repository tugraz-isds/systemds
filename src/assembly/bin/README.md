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
{% endcomment %}
-->

# SystemDS

###Overview:
 SystemDS is a versatile system for the end-to-end data science lifecycle from data integration, cleaning, and feature engineering, over efficient, local and distributed ML model training, to deployment and serving. To this end, we aim to provide a stack of declarative languages with R-like syntax for (1) the different tasks of the data-science lifecycle, and (2) users with different expertise. These high-level scripts are compiled into hybrid execution plans of local, in-memory CPU and GPU operations, as well as distributed operations on Apache Spark. In contrast to existing systems - that either provide homogeneous tensors or 2D Datasets - and in order to serve the entire data science lifecycle, the underlying data model are DataTensors, i.e., tensors (multi-dimensional arrays) whose first dimension may have a heterogeneous and nested schema.

**Documentation:** [SystemDS Documentation](https://github.com/tugraz-isds/systemds/tree/master/docs)<br/>

####Getting started:

Create a text file called hello.dml containing the content 
 ```shell script
X = rand(rows=$1, cols=$2, min=0, max=10, sparsity=$3)
Y = rand(rows=$2, cols=$1, min=0, max=10, sparsity=$3)
Z = X %*% Y
print("Your hello world matrix contains:")
print(toString(Z))
write(Z, "Z")
``` 

Now run that first script you created by running one of the following commands depending on your operating system:

#####Linux/Bash: 
```shell script
$ runStandaloneSystemDS.sh hello.dml -args 10 10 1.0
```

#####Windows/CMD: 
```shell script
$ runStandaloneSystemDS.bat hello.dml -args 10 10 1.0
```

**Explaination:** The script takes three parameters for the creation of your matrices X and Y: rows, columns and degree of sparsity.
As you can see, DML can access these parameters by specifying $1, $2, ... etc

The output should read something similar to this (the warning can be safely ignored):
```shell script
20/03/09 16:40:29 INFO api.DMLScript: BEGIN DML run 03/09/2020 16:40:29
20/03/09 16:40:30 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Your hello world matrix contains:
250,902 207,246 305,621 182,892 234,394 258,587 132,225 259,684 255,774 228,338
296,740 261,475 379,148 156,640 304,543 267,684 191,867 258,826 373,633 276,497
215,877 186,171 332,165 201,091 289,179 265,160 125,890 289,836 320,434 287,394
389,057 332,681 336,182 285,432 310,218 340,838 301,308 354,130 410,698 282,453
325,903 264,745 377,086 242,436 277,836 285,519 190,167 358,228 332,295 288,034
360,858 301,739 398,514 265,299 333,124 321,178 240,755 299,871 428,856 300,128
368,983 291,729 303,091 191,586 231,050 280,335 266,906 278,203 395,130 203,706
173,610 114,076 157,683 140,927 145,605 145,654 143,674 192,044 196,735 166,428
310,329 258,840 286,302 231,136 305,804 300,016 266,434 297,557 392,566 281,211
249,234 196,488 216,662 180,294 165,482 169,318 172,686 204,275 296,595 148,888

SystemDS Statistics:
Total execution time:           0,122 sec.

20/03/09 16:40:30 INFO api.DMLScript: END DML run 03/09/2020 16:40:30
```
 