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

package org.tugraz.sysds.test.functions.indexing;

import java.util.HashMap;

import org.junit.Test;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.runtime.meta.MatrixCharacteristics;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class LeftIndexingUpdateInPlaceTest extends AutomatedTestBase
{
	private final static String TEST_DIR = "functions/indexing/";
	private final static String TEST_NAME = "LeftIndexingUpdateInPlaceTest";
	private final static String TEST_NAME_BUG_TEST1 = "LeftIndexingUpdateInPlaceTestBugTest1";
	private final static String TEST_NAME_BUG_TEST2 = "LeftIndexingUpdateInPlaceTestBugTest2";
	private final static String TEST_CLASS_DIR = TEST_DIR + LeftIndexingUpdateInPlaceTest.class.getSimpleName() + "/";
	
	private final static int rows1 = 1281;
	private final static int cols1 = 1102;
	private final static int cols2 = 226;
	private final static int cols3 = 1;
	
	private final static double sparsity1 = 0.05;
	private final static double sparsity2 = 0.9;
	
	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] {"R"}));
		addTestConfiguration(TEST_NAME_BUG_TEST1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME_BUG_TEST1, new String[] {"R"}));
		addTestConfiguration(TEST_NAME_BUG_TEST2, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME_BUG_TEST2, new String[] {"R"}));
	}
	
	@Test
	public void testSparseMatrixSparseMatrix() {
		runLeftIndexingUpdateInPlaceTest(true, true, false, false, TEST_NAME);
	}
	
	@Test
	public void testSparseMatrixSparseVector() {
		runLeftIndexingUpdateInPlaceTest(true, true, true, false, TEST_NAME);
	}
	
	@Test
	public void testSparseMatrixDenseMatrix() {
		runLeftIndexingUpdateInPlaceTest(true, false, false, false, TEST_NAME);
	}
	
	@Test
	public void testSparseMatrixDenseVector() {
		runLeftIndexingUpdateInPlaceTest(true, false, true, false, TEST_NAME);
	}
	
	@Test
	public void testDenseMatrixSparseMatrix() {
		runLeftIndexingUpdateInPlaceTest(false, true, false, false, TEST_NAME);
	}
	
	@Test
	public void testDenseMatrixSparseVector() {
		runLeftIndexingUpdateInPlaceTest(false, true, true, false, TEST_NAME);
	}
	
	@Test
	public void testDenseMatrixDenseMatrix() {
		runLeftIndexingUpdateInPlaceTest(false, false, false, false, TEST_NAME);
	}
	
	@Test
	public void testDenseMatrixDenseVector() {
		runLeftIndexingUpdateInPlaceTest(false, false, true, false, TEST_NAME);
	}
	
	@Test
	public void testSparseMatrixEmptyMatrix() {
		runLeftIndexingUpdateInPlaceTest(true, true, false, true, TEST_NAME);
	}
	
	@Test
	public void testSparseMatrixEmptyVector() {
		runLeftIndexingUpdateInPlaceTest(true, true, true, true, TEST_NAME);
	}
	
	@Test
	public void testDenseMatrixEmptyMatrix() {
		runLeftIndexingUpdateInPlaceTest(false, true, false, true, TEST_NAME);
	}
	
	@Test
	public void testDenseMatrixEmptyVector() {
		runLeftIndexingUpdateInPlaceTest(false, true, true, true, TEST_NAME);
	}

	@Test
	public void testDenseMatrixDenseMatrixBugTest1() {
		runLeftIndexingUpdateInPlaceTest(false, false, false, false, TEST_NAME_BUG_TEST1);
	}

	@Test
	public void testDenseMatrixDenseMatrixBugTest2() {
		runLeftIndexingUpdateInPlaceTest(false, false, false, false, TEST_NAME_BUG_TEST2);
	}

	/**
	 * 
	 * @param sparseM1
	 * @param sparseM2
	 * @param vectorM2
	 */
	public void runLeftIndexingUpdateInPlaceTest(boolean sparseM1, boolean sparseM2, boolean vectorM2, boolean emptyM2, String testName)
	{
		ExecMode oldRTP = rtplatform;
		rtplatform = ExecMode.HYBRID;
		
		try {
			TestConfiguration config = getTestConfiguration(testName);
			loadTestConfiguration(config);
			
			double spM1 = sparseM1 ? sparsity1 : sparsity2;
			double spM2 = emptyM2 ? 0 : (sparseM2 ? sparsity1 : sparsity2);
			int colsM2 = vectorM2 ? cols3 : cols2;
			
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + testName + ".dml";
			programArgs = new String[]{"-explain","-args", input("A"), input("B"), output("R")};
			
			fullRScriptName = HOME + testName + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + 
				inputDir() + " " + expectedDir();
			
			//generate input data sets
			double[][] A = getRandomMatrix(rows1, cols1, -1, 1, spM1, 1234);
			writeInputMatrixWithMTD("A", A, true);
			double[][] B = getRandomMatrix(rows1, colsM2, -1, 1, spM2, 5678);
			writeInputMatrixWithMTD("B", B, true);

			//run dml and r script
			runTest(true, false, null, 2); //2xrblk
			if(testName == TEST_NAME) {
				runRScript(true);

				HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("R");
				HashMap<CellIndex, Double> rfile = readRMatrixFromFS("R");
				TestUtils.compareMatrices(dmlfile, rfile, 0, "DML", "R");
				checkDMLMetaDataFile("R", new MatrixCharacteristics(rows1, cols1, 1, 1));
			}
		}
		finally {
			rtplatform = oldRTP;
		}
	}
}
