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

package org.tugraz.sysds.test.functions.ternary;

import java.util.HashMap;

import org.junit.Test;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.hops.TernaryOp;
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

/**
 * This test investigates the specific Hop-Lop rewrite for the following pattern:
 * 
 * IA = (A != 0) * seq (1, nrow (A), 1);
 * IA = matrix (IA, rows = (nrow (A) * ncol(A)), cols = 1, byrow = FALSE);
 * VA = matrix ( A, rows = (nrow (A) * ncol(A)), cols = 1, byrow = FALSE);
 * IA = removeEmpty (target = IA, margin = "rows");
 * VA = removeEmpty (target = VA, margin = "rows");
 * H = table (IA, VA, nrow(A), max(A));
 * 
 */
public class CTableMatrixIgnoreZerosTest extends AutomatedTestBase 
{
	
	private final static String TEST_NAME1 = "CTableRowHist";
	
	private final static String TEST_DIR = "functions/ternary/";
	private final static String TEST_CLASS_DIR = TEST_DIR + CTableMatrixIgnoreZerosTest.class.getSimpleName() + "/";
	private final static double eps = 1e-10;
	
	private final static int rows = 1456;
	private final static int cols = 345;
	private final static double sparsity1 = 0.7;
	private final static double sparsity2 = 0.07;
	
	@Override
	public void setUp() 
	{
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] { "B" })   ); 
	}

	@Test
	public void testCTableMatrixIgnoreZerosRewriteDenseSP() 
	{
		runCTableTest(true, false, ExecType.SPARK);
	}
	
	@Test
	public void testCTableMatrixIgnoreZerosRewriteSparseSP() 
	{
		runCTableTest(true, true, ExecType.SPARK);
	}
	
	@Test
	public void testCTableMatrixIgnoreZerosNoRewriteDenseSP() 
	{
		runCTableTest(false, false, ExecType.SPARK);
	}
	
	@Test
	public void testCTableMatrixIgnoreZerosNoRewriteSparseSP() 
	{
		runCTableTest(false, true, ExecType.SPARK);
	}
	
	@Test
	public void testCTableMatrixIgnoreZerosRewriteDenseCP() 
	{
		runCTableTest(true, false, ExecType.CP);
	}
	
	@Test
	public void testCTableMatrixIgnoreZerosRewriteSparseCP() 
	{
		runCTableTest(true, true, ExecType.CP);
	}
	
	@Test
	public void testCTableMatrixIgnoreZerosNoRewriteDenseCP() 
	{
		runCTableTest(false, false, ExecType.CP);
	}
	
	@Test
	public void testCTableMatrixIgnoreZerosNoRewriteSparseCP() 
	{
		runCTableTest(false, true, ExecType.CP);
	}
	
	private void runCTableTest( boolean rewrite, boolean sparse, ExecType et)
	{
		String TEST_NAME = TEST_NAME1;
		
		//rtplatform for MR
		ExecMode platformOld = rtplatform;
		boolean rewriteOld = TernaryOp.ALLOW_CTABLE_SEQUENCE_REWRITES;
		
		switch( et ){
			case SPARK: rtplatform = ExecMode.SPARK; break;
			default: rtplatform = ExecMode.HYBRID; break;
		}
	
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		if( rtplatform == ExecMode.SPARK )
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;
	
		TernaryOp.ALLOW_CTABLE_SEQUENCE_REWRITES = rewrite;
		
		double sparsity = sparse ? sparsity2: sparsity1;
		
		try
		{
			TestConfiguration config = getTestConfiguration(TEST_NAME);
			loadTestConfiguration(config);
			
			/* This is for running the junit test the new way, i.e., construct the arguments directly */
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-args", input("A"), output("B")};
			
			fullRScriptName = HOME + TEST_NAME + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " + expectedDir();
			
			//generate actual dataset (always dense because values <=0 invalid)
			double[][] A = getRandomMatrix(rows, cols, 1, 10, sparsity, 7); 
			writeInputMatrixWithMTD("A", A, true);
	
			runTest(true, false, null, -1); 
			runRScript(true); 
			
			//compare matrices 
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("B");
			HashMap<CellIndex, Double> rfile  = readRMatrixFromFS("B");
			TestUtils.compareMatrices(dmlfile, rfile, eps, "Stat-DML", "Stat-R");
		}
		finally
		{
			rtplatform = platformOld;
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
			TernaryOp.ALLOW_CTABLE_SEQUENCE_REWRITES = rewriteOld;
		}
	}
}