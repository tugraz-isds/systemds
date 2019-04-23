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

package org.tugraz.sysds.test.functions.aggregate;

import java.util.HashMap;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;
import org.tugraz.sysds.utils.Statistics;

/**
 * 
 * 
 */
public class FullAggregateTest extends AutomatedTestBase 
{
	private final static String TEST_NAME1 = "AllSum";
	private final static String TEST_NAME2 = "AllMean";
	private final static String TEST_NAME3 = "AllMax";
	private final static String TEST_NAME4 = "AllMin";
	private final static String TEST_NAME5 = "AllProd";
	private final static String TEST_NAME6 = "DiagSum"; //trace

	private final static String TEST_DIR = "functions/aggregate/";
	private static final String TEST_CLASS_DIR = TEST_DIR + FullAggregateTest.class.getSimpleName() + "/";
	private final static double eps = 1e-10;
	
	private final static int rows1 = 1005;
	private final static int cols1 = 1;
	private final static int cols2 = 1079;
	private final static double sparsity1 = 0.1;
	private final static double sparsity2 = 0.7;
	
	private enum OpType{
		SUM,
		MEAN,
		MAX,
		MIN,
		PROD,
		TRACE
	}
	
	@Override
	public void setUp() 
	{
		addTestConfiguration(TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[]{"B"}));
		addTestConfiguration(TEST_NAME2, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2, new String[]{"B"}));
		addTestConfiguration(TEST_NAME3, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME3, new String[]{"B"}));
		addTestConfiguration(TEST_NAME4, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME4, new String[]{"B"}));
		addTestConfiguration(TEST_NAME5, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME5, new String[]{"B"}));
		addTestConfiguration(TEST_NAME6, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME6, new String[]{"B"}));

		if (TEST_CACHE_ENABLED) {
			setOutAndExpectedDeletionDisabled(true);
		}
	}

	@BeforeClass
	public static void init()
	{
		TestUtils.clearDirectory(TEST_DATA_DIR + TEST_CLASS_DIR);
	}

	@AfterClass
	public static void cleanUp()
	{
		if (TEST_CACHE_ENABLED) {
			TestUtils.clearDirectory(TEST_DATA_DIR + TEST_CLASS_DIR);
		}
	}
	
	@Test
	public void testSumDenseMatrixCP() 
	{
		runColAggregateOperationTest(OpType.SUM, false, false, ExecType.CP);
	}
	
	@Test
	public void testMeanDenseMatrixCP() 
	{
		runColAggregateOperationTest(OpType.MEAN, false, false, ExecType.CP);
	}	
	
	@Test
	public void testMaxDenseMatrixCP() 
	{
		runColAggregateOperationTest(OpType.MAX, false, false, ExecType.CP);
	}
	
	@Test
	public void testMinDenseMatrixCP() 
	{
		runColAggregateOperationTest(OpType.MIN, false, false, ExecType.CP);
	}
	
	@Test
	public void testProdDenseMatrixCP() 
	{
		runColAggregateOperationTest(OpType.PROD, false, false, ExecType.CP);
	}
	
	@Test
	public void testTraceDenseMatrixCP() 
	{
		runColAggregateOperationTest(OpType.TRACE, false, false, ExecType.CP);
	}
	
	@Test
	public void testSumDenseVectorCP() 
	{
		runColAggregateOperationTest(OpType.SUM, false, true, ExecType.CP);
	}
	
	@Test
	public void testMeanDenseVectorCP() 
	{
		runColAggregateOperationTest(OpType.MEAN, false, true, ExecType.CP);
	}	
	
	@Test
	public void testMaxDenseVectorCP() 
	{
		runColAggregateOperationTest(OpType.MAX, false, true, ExecType.CP);
	}
	
	@Test
	public void testMinDenseVectorCP() 
	{
		runColAggregateOperationTest(OpType.MIN, false, true, ExecType.CP);
	}
	
	@Test
	public void testProdDenseVectorCP() 
	{
		runColAggregateOperationTest(OpType.PROD, false, true, ExecType.CP);
	}
	
	@Test
	public void testTraceDenseVectorCP() 
	{
		runColAggregateOperationTest(OpType.TRACE, false, true, ExecType.CP);
	}
	
	@Test
	public void testSumSparseMatrixCP() 
	{
		runColAggregateOperationTest(OpType.SUM, true, false, ExecType.CP);
	}
	
	@Test
	public void testMeanSparseMatrixCP() 
	{
		runColAggregateOperationTest(OpType.MEAN, true, false, ExecType.CP);
	}	
	
	@Test
	public void testMaxSparseMatrixCP() 
	{
		runColAggregateOperationTest(OpType.MAX, true, false, ExecType.CP);
	}
	
	@Test
	public void testMinSparseMatrixCP() 
	{
		runColAggregateOperationTest(OpType.MIN, true, false, ExecType.CP);
	}
	
	@Test
	public void testProdSparseMatrixCP() 
	{
		runColAggregateOperationTest(OpType.PROD, true, false, ExecType.CP);
	}
	
	@Test
	public void testTraceSparseMatrixCP() 
	{
		runColAggregateOperationTest(OpType.TRACE, true, false, ExecType.CP);
	}
	
	@Test
	public void testSumSparseVectorCP() 
	{
		runColAggregateOperationTest(OpType.SUM, true, true, ExecType.CP);
	}
	
	@Test
	public void testMeanSparseVectorCP() 
	{
		runColAggregateOperationTest(OpType.MEAN, true, true, ExecType.CP);
	}	
	
	@Test
	public void testMaxSparseVectorCP() 
	{
		runColAggregateOperationTest(OpType.MAX, true, true, ExecType.CP);
	}
	
	@Test
	public void testMinSparseVectorCP() 
	{
		runColAggregateOperationTest(OpType.MIN, true, true, ExecType.CP);
	}
	
	@Test
	public void testProdSparseVectorCP() 
	{
		runColAggregateOperationTest(OpType.PROD, true, true, ExecType.CP);
	}
	
	@Test
	public void testTraceSparseVectorCP() 
	{
		runColAggregateOperationTest(OpType.TRACE, true, true, ExecType.CP);
	}
	
	@Test
	public void testSumDenseMatrixSP() 
	{
		runColAggregateOperationTest(OpType.SUM, false, false, ExecType.SPARK);
	}
	
	@Test
	public void testMeanDenseMatrixSP() 
	{
		runColAggregateOperationTest(OpType.MEAN, false, false, ExecType.SPARK);
	}	
	
	@Test
	public void testMaxDenseMatrixSP() 
	{
		runColAggregateOperationTest(OpType.MAX, false, false, ExecType.SPARK);
	}
	
	@Test
	public void testMinDenseMatrixSP() 
	{
		runColAggregateOperationTest(OpType.MIN, false, false, ExecType.SPARK);
	}
	
	@Test
	public void testProdDenseMatrixSP() 
	{
		runColAggregateOperationTest(OpType.PROD, false, false, ExecType.SPARK);
	}
	
	@Test
	public void testTraceDenseMatrixSP() 
	{
		runColAggregateOperationTest(OpType.TRACE, false, false, ExecType.SPARK);
	}
	
	@Test
	public void testSumDenseVectorSP() 
	{
		runColAggregateOperationTest(OpType.SUM, false, true, ExecType.SPARK);
	}
	
	@Test
	public void testMeanDenseVectorSP() 
	{
		runColAggregateOperationTest(OpType.MEAN, false, true, ExecType.SPARK);
	}	
	
	@Test
	public void testMaxDenseVectorSP() 
	{
		runColAggregateOperationTest(OpType.MAX, false, true, ExecType.SPARK);
	}
	
	@Test
	public void testMinDenseVectorSP() 
	{
		runColAggregateOperationTest(OpType.MIN, false, true, ExecType.SPARK);
	}
	
	@Test
	public void testProdDenseVectorSP() 
	{
		runColAggregateOperationTest(OpType.PROD, false, true, ExecType.SPARK);
	}
	
	@Test
	public void testTraceDenseVectorSP() 
	{
		runColAggregateOperationTest(OpType.TRACE, false, true, ExecType.SPARK);
	}
	
	@Test
	public void testSumSparseMatrixSP() 
	{
		runColAggregateOperationTest(OpType.SUM, true, false, ExecType.SPARK);
	}
	
	@Test
	public void testMeanSparseMatrixSP() 
	{
		runColAggregateOperationTest(OpType.MEAN, true, false, ExecType.SPARK);
	}	
	
	@Test
	public void testMaxSparseMatrixSP() 
	{
		runColAggregateOperationTest(OpType.MAX, true, false, ExecType.SPARK);
	}
	
	@Test
	public void testMinSparseMatrixSP() 
	{
		runColAggregateOperationTest(OpType.MIN, true, false, ExecType.SPARK);
	}
	
	@Test
	public void testProdSparseMatrixSP() 
	{
		runColAggregateOperationTest(OpType.PROD, true, false, ExecType.SPARK);
	}
	
	@Test
	public void testTraceSparseMatrixSP() 
	{
		runColAggregateOperationTest(OpType.TRACE, true, false, ExecType.SPARK);
	}
	
	@Test
	public void testSumSparseVectorSP() 
	{
		runColAggregateOperationTest(OpType.SUM, true, true, ExecType.SPARK);
	}
	
	@Test
	public void testMeanSparseVectorSP() 
	{
		runColAggregateOperationTest(OpType.MEAN, true, true, ExecType.SPARK);
	}	
	
	@Test
	public void testMaxSparseVectorSP() 
	{
		runColAggregateOperationTest(OpType.MAX, true, true, ExecType.SPARK);
	}
	
	@Test
	public void testMinSparseVectorSP() 
	{
		runColAggregateOperationTest(OpType.MIN, true, true, ExecType.SPARK);
	}
	
	@Test
	public void testProdSparseVectorSP() 
	{
		runColAggregateOperationTest(OpType.PROD, true, true, ExecType.SPARK);
	}
	
	@Test
	public void testTraceSparseVectorSP() 
	{
		runColAggregateOperationTest(OpType.TRACE, true, true, ExecType.SPARK);
	}
	
	
	/**
	 * 
	 * @param sparseM1
	 * @param sparseM2
	 * @param instType
	 */
	private void runColAggregateOperationTest( OpType type, boolean sparse, boolean vector, ExecType instType)
	{
		ExecMode platformOld = rtplatform;
		switch( instType ){
			case SPARK: rtplatform = ExecMode.SPARK; break;
			default: rtplatform = ExecMode.HYBRID; break;
		}
	
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		if( rtplatform == ExecMode.SPARK )
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;

		try
		{
			String TEST_NAME = null;
			switch( type )
			{
				case SUM: TEST_NAME = TEST_NAME1; break;
				case MEAN: TEST_NAME = TEST_NAME2; break;
				case MAX: TEST_NAME = TEST_NAME3; break;
				case MIN: TEST_NAME = TEST_NAME4; break;
				case PROD: TEST_NAME = TEST_NAME5; break;
				case TRACE: TEST_NAME = TEST_NAME6; break;
			}
			
			int cols = (vector) ? cols1 : cols2;
			int rows = (type==OpType.TRACE) ? cols : rows1;
			double sparsity = (sparse) ? sparsity1 : sparsity2;
			
			String TEST_CACHE_DIR = "";
			if (TEST_CACHE_ENABLED)
			{
				TEST_CACHE_DIR = type.ordinal() + "_" + rows + "_" + cols + "_" + sparsity + "/";
			}
			
			TestConfiguration config = getTestConfiguration(TEST_NAME);
			loadTestConfiguration(config, TEST_CACHE_DIR);
			
			/* This is for running the junit test the new way, i.e., construct the arguments directly */
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-args", input("A"),
				Integer.toString(rows), Integer.toString(cols), output("B") };
			
			fullRScriptName = HOME + TEST_NAME + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " + expectedDir();
	
			//generate actual dataset 
			double[][] A = getRandomMatrix(rows, cols, -0.05, 1, sparsity, 7); 
			writeInputMatrix("A", A, true);
	
			runTest(true, false, null, -1); 
			if( instType==ExecType.CP ) //in CP no MR jobs should be executed
				Assert.assertEquals("Unexpected number of executed MR jobs.", 0, Statistics.getNoOfExecutedSPInst());
		
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
		}
	}
}
