/*
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

package org.tugraz.sysds.test.functions.binary.matrix;

import org.junit.Test;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.runtime.meta.MatrixCharacteristics;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;

public class MatrixVectorTest extends AutomatedTestBase 
{

	private final static String TEST_NAME1 = "MatrixVectorMultiplication";
	private final static String TEST_DIR = "functions/binary/matrix/";
	private final static String TEST_CLASS_DIR = TEST_DIR + MatrixVectorTest.class.getSimpleName() + "/";
	private final static double eps = 1e-10;
	
	private final static int rows = 3500;
	private final static int cols_wide = 1500;
	private final static int cols_skinny = 500;
	
	private final static double sparsity1 = 0.7;
	private final static double sparsity2 = 0.1;
	
	@Override
	public void setUp() 
	{
		addTestConfiguration(TEST_NAME1, 
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] { "y" }) ); 
	}
	
	@Test
	public void testMVwideSparseCP() 
	{
		runMatrixVectorMultiplicationTest(cols_wide, ExecMode.SINGLE_NODE, true);
	}
	
	@Test
	public void testMVwideSparseHYBRID() 
	{
		runMatrixVectorMultiplicationTest(cols_wide, ExecMode.HYBRID, true);
	}
	
	@Test
	public void testMVwideDenseCP() 
	{
		runMatrixVectorMultiplicationTest(cols_wide, ExecMode.SINGLE_NODE, false);
	}

	@Test
	public void testMVwideDenseHYBRID() 
	{
		runMatrixVectorMultiplicationTest(cols_wide, ExecMode.HYBRID, false);
	}
	
	@Test
	public void testMVskinnySparseCP() 
	{
		runMatrixVectorMultiplicationTest(cols_skinny, ExecMode.SINGLE_NODE, true);
	}

	@Test
	public void testMVskinnySparseHYBRID() 
	{
		runMatrixVectorMultiplicationTest(cols_skinny, ExecMode.HYBRID, true);
	}
	
	@Test
	public void testMVskinnyDenseCP() 
	{
		runMatrixVectorMultiplicationTest(cols_skinny, ExecMode.SINGLE_NODE, false);
	}

	@Test
	public void testMVskinnyDenseHYBRID() 
	{
		runMatrixVectorMultiplicationTest(cols_skinny, ExecMode.HYBRID, false);
	}
	
	private void runMatrixVectorMultiplicationTest( int cols, ExecMode rt, boolean sparse )
	{

		ExecMode rtold = rtplatform;
		rtplatform = rt;

		try
		{
			getAndLoadTestConfiguration(TEST_NAME1);
			
			/* This is for running the junit test the new way, i.e., construct the arguments directly */
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME1 + ".dml";
			programArgs = new String[]{"-args", input("A"), input("x"), output("y")};
			
			fullRScriptName = HOME + TEST_NAME1 + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " + expectedDir();
	
			//generate actual dataset
			double[][] A = getRandomMatrix(rows, cols, 0, 1, sparse?sparsity2:sparsity1, 10); 
			MatrixCharacteristics mc = new MatrixCharacteristics(rows, cols, -1, -1, -1);
			writeInputMatrixWithMTD("A", A, true, mc);
			double[][] x = getRandomMatrix(cols, 1, 0, 1, 1.0, 10); 
			mc = new MatrixCharacteristics(cols, 1, -1, -1, cols);
			writeInputMatrixWithMTD("x", x, true, mc);
	
			boolean exceptionExpected = false;
			runTest(true, exceptionExpected, null, -1); 
			runRScript(true);
			
			compareResultsWithR(eps);
			
		}
		finally
		{
			rtplatform = rtold;
		}
	}
	
}