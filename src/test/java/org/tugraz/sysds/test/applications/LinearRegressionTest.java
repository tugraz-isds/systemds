/*
 * Modifications Copyright 2019 Graz University of Technology
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
 
package org.tugraz.sysds.test.applications;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestUtils;

@RunWith(value = Parameterized.class)
public class LinearRegressionTest extends AutomatedTestBase {
	
	protected final static String TEST_DIR = "applications/linear_regression/";
	protected final static String TEST_NAME = "LinearRegression";
	protected String TEST_CLASS_DIR = TEST_DIR + LinearRegressionTest.class.getSimpleName() + "/";

	protected int numRecords, numFeatures;
	protected double sparsity;
	
	public LinearRegressionTest(int rows, int cols, double sp) {
		numRecords = rows;
		numFeatures = cols;
		sparsity = sp;
	}
	
	@Parameters
	 public static Collection<Object[]> data() {
	   Object[][] data = new Object[][] { 
			   //sparse tests (sparsity=0.01)
			   {100, 50, 0.01}, {1000, 500, 0.01}, {10000, 750, 0.01}, {100000, 1000, 0.01},
			   //dense tests (sparsity=0.7)
			   {100, 50, 0.7}, {1000, 500, 0.7}, {10000, 750, 0.7} };
	   return Arrays.asList(data);
	 }
	 
	@Override
	public void setUp()
	{
		addTestConfiguration(TEST_CLASS_DIR, TEST_NAME);
	}
	
	@Test
	public void testLinearRegression() {
		System.out.println("------------ BEGIN " + TEST_NAME + " TEST WITH {" + numRecords + ", " + numFeatures
				+ ", " + sparsity + "} ------------");
		
		int rows = numRecords;
		int cols = numFeatures;
		
		getAndLoadTestConfiguration(TEST_NAME);
		
		List<String> proArgs = new ArrayList<String>();
		
		proArgs.add("-stats");
		proArgs.add("-args");
		proArgs.add(input("v"));
		proArgs.add(input("y"));
		proArgs.add(Double.toString(Math.pow(10, -8)));
		proArgs.add(output("w"));
		programArgs = proArgs.toArray(new String[proArgs.size()]);
		
		fullDMLScriptName = getScript();
		
		rCmd = getRCmd(inputDir(), Double.toString(Math.pow(10, -8)), expectedDir());

		double[][] v = getRandomMatrix(rows, cols, 0, 1, sparsity, -1);
		double[][] y = getRandomMatrix(rows, 1, 1, 10, 1, -1);
		writeInputMatrixWithMTD("v", v, true);
		writeInputMatrixWithMTD("y", y, true);
		
		/*
		 * Expected number of jobs:
		 * Rand - 1 job 
		 * Computation before while loop - 4 jobs
		 * While loop iteration - 10 jobs
		 * Final output write - 1 job
		 */
		int expectedNumberOfJobs = 16;
		runTest(true, EXCEPTION_NOT_EXPECTED, null, expectedNumberOfJobs);
		
		runRScript(true);
		
		HashMap<CellIndex, Double> wR = readRMatrixFromFS("w");
		HashMap<CellIndex, Double> wSYSTEMDS= readDMLMatrixFromHDFS("w");
		TestUtils.compareMatrices(wR, wSYSTEMDS, Math.pow(10, -10), "wR", "wSYSTEMDS");
	}
}
