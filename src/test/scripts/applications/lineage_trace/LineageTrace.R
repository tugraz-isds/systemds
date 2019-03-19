#-------------------------------------------------------------
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

# JUnit test class: dml.test.integration.applications.LineageTraceTest.java
# command line invocation assuming $LR_HOME is set to the home of the R script
# Rscript $LR_HOME/LineageTrace.R $LR_HOME/in/ $LR_HOME/expected/
args <- commandArgs(TRUE);

library("Matrix");

X = readMM(paste(args[1], "X.mtx", sep=""));

max_iteration = 3;
i = 0;

while(i < max_iteration) {
	if (i == 1)
    {
        X = X * 3;
    }
    else
    {
        X = X + 7;
    }
    i = i + 1;
}

Y = t(X) %*% X;

writeMM(as(X,"CsparseMatrix"), paste(args[2], "X", sep=""));
writeMM(as(Y,"CsparseMatrix"), paste(args[2], "Y", sep=""));