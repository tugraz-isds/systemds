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

package org.tugraz.sysds.test.functions.misc;

import java.util.HashMap;

import org.junit.Test;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

/**
 * 
 * 
 */
public class RewriteSlicedMatrixMultTest extends AutomatedTestBase 
{
	
	private static final String TEST_NAME1 = "RewriteSlicedMatrixMult";
	private static final String TEST_DIR = "functions/misc/";
	private static final String TEST_CLASS_DIR = TEST_DIR + RewriteSlicedMatrixMultTest.class.getSimpleName() + "/";
	
	private static final int dim1 = 1234;
	private static final int dim2 = 567;
	private static final int dim3 = 1023;
	private static final double sparsity1 = 0.7;
	private static final double sparsity2 = 0.3;
	private static final double eps = Math.pow(10, -10);
	
	@Override
	public void setUp() 
	{
		TestUtils.clearAssertionInformation();
		addTestConfiguration( TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] { "R" }) );
	}

	@Test
	public void testRewriteSlicedMatrixMultDenseNoRewrite()  {
		testRewriteSlicedMatrixMult( TEST_NAME1, false, false );
	}
	
	@Test
	public void testRewriteSlicedMatrixMultSparseNoRewrite()  {
		testRewriteSlicedMatrixMult( TEST_NAME1, true, false );
	}
	
	@Test
	public void testRewriteSlicedMatrixMultDenseRewrite()  {
		testRewriteSlicedMatrixMult( TEST_NAME1, false, true );
	}
	
	@Test
	public void testRewriteSlicedMatrixMultSparseRewrite()  {
		testRewriteSlicedMatrixMult( TEST_NAME1, true, true );
	}
	
	
	/**
	 * 
	 * @param condition
	 * @param branchRemoval
	 * @param IPA
	 */
	private void testRewriteSlicedMatrixMult( String testname, boolean sparse, boolean rewrites )
	{	
		boolean oldFlag = OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION;
		
		try
		{
			TestConfiguration config = getTestConfiguration(testname);
			loadTestConfiguration(config);
			
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + testname + ".dml";
			programArgs = new String[]{ "-stats","-args", 
				input("A"), input("B"), output("R") };
			
			fullRScriptName = HOME + testname + ".R";
			rCmd = getRCmd(inputDir(), expectedDir());			

			OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = rewrites;

			double sparsity = sparse ? sparsity2 : sparsity1;
			double[][] A = getRandomMatrix(dim1, dim2, -1, 1, sparsity, 7);
			writeInputMatrixWithMTD("A", A, true);
			
			double[][] B = getRandomMatrix(dim2, dim3, -1, 1, sparsity, 3);
			writeInputMatrixWithMTD("B", B, true);
			
			//run performance tests
			runTest(true, false, null, -1); 
			runRScript(true); 
			
			//compare matrices 
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("R");
			HashMap<CellIndex, Double> rfile  = readRMatrixFromFS("R");
			TestUtils.compareMatrices(dmlfile, rfile, eps, "Stat-DML", "Stat-R");
		}
		finally
		{
			OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = oldFlag;
		}
	}	
}