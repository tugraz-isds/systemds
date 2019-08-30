#!/usr/bin/bash
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

jars='systemds-*-extra.jar'

# N = Number of images, C = number of channels, H = height, W = width
# F = number of filters, Hf = filter height, Wf = filter width
N=5
C=3
H=28
W=28
F=32 
Hf=3 
Wf=3
for sparsity in  0.1 0.2 0.5 0.6 0.9
do
	for stride in 1 2 3
	do
		for pad in 0 1 2
		do
			# Generating the data
			$SPARK_HOME/bin/spark-submit SystemDS.jar -f gen_conv2d_bwd_filter.dml -nvargs sp=$sparsity N=$N C=$C H=$H W=$W F=$F Hf=$Hf Wf=$Wf stride=$stride pad=$pad
			# Running a test in CPU mode
			$SPARK_HOME/bin/spark-submit SystemDS.jar -f test_conv2d_bwd_filter.dml -nvargs stride=$stride pad=$pad out=out_cp.csv N=$N C=$C H=$H W=$W F=$F Hf=$Hf Wf=$Wf
			# Running a test in GPU mode
			$SPARK_HOME/bin/spark-submit --jars $jars SystemDS.jar -f test_conv2d_bwd_filter.dml -stats -gpu force -nvargs stride=$stride pad=$pad out=out_gpu.csv N=$N C=$C H=$H W=$W F=$F Hf=$Hf Wf=$Wf
			# Comparing the CPU vs GPU results to make sure they are the same
			$SPARK_HOME/bin/spark-submit SystemDS.jar -f compare.dml -args out_cp.csv out_gpu.csv "conv2d_backward_filter:stride="$stride",pad="$pad",sparsity="$sparsity
			rm -rf out_cp.csv out_gpu.csv out_cp.csv.mtd out_gpu.csv.mtd
		done
	done
	rm -rf input.mtx input.mtx.mtd
done
