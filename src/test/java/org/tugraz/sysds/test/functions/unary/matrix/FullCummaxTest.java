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

package org.tugraz.sysds.test.functions.unary.matrix;

import java.util.HashMap;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.hops.OptimizerUtils;
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
public class FullCummaxTest extends AutomatedTestBase 
{
	private final static String TEST_NAME = "Cummax";
	private final static String TEST_DIR = "functions/unary/matrix/";
	private static final String TEST_CLASS_DIR = TEST_DIR + FullCummaxTest.class.getSimpleName() + "/";
	
	private final static double eps = 1e-10;
	
	private final static int rowsMatrix = 1201;
	private final static int colsMatrix = 1103;
	private final static double spSparse = 0.1;
	private final static double spDense = 0.9;
	
	private enum InputType {
		COL_VECTOR,
		ROW_VECTOR,
		MATRIX
	}
	
	@Override
	public void setUp() 
	{
		addTestConfiguration(TEST_NAME,new TestConfiguration(TEST_CLASS_DIR, TEST_NAME,new String[]{"B"})); 

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
	public void testCummaxColVectorDenseCP() 
	{
		runColAggregateOperationTest(InputType.COL_VECTOR, false, ExecType.CP);
	}
	
	@Test
	public void testCummaxRowVectorDenseCP() 
	{
		runColAggregateOperationTest(InputType.ROW_VECTOR, false, ExecType.CP);
	}
	
	@Test
	public void testCummaxRowVectorDenseNoRewritesCP() 
	{
		runColAggregateOperationTest(InputType.ROW_VECTOR, false, ExecType.CP, false);
	}
	
	@Test
	public void testCummaxMatrixDenseCP() 
	{
		runColAggregateOperationTest(InputType.MATRIX, false, ExecType.CP);
	}
	
	@Test
	public void testCummaxColVectorSparseCP() 
	{
		runColAggregateOperationTest(InputType.COL_VECTOR, true, ExecType.CP);
	}
	
	@Test
	public void testCummaxRowVectorSparseCP() 
	{
		runColAggregateOperationTest(InputType.ROW_VECTOR, true, ExecType.CP);
	}
	
	@Test
	public void testCummaxRowVectorSparseNoRewritesCP() 
	{
		runColAggregateOperationTest(InputType.ROW_VECTOR, true, ExecType.CP, false);
	}
	
	@Test
	public void testCummaxMatrixSparseCP() 
	{
		runColAggregateOperationTest(InputType.MATRIX, true, ExecType.CP);
	}
	
	@Test
	public void testCummaxColVectorDenseSP() 
	{
		runColAggregateOperationTest(InputType.COL_VECTOR, false, ExecType.SPARK);
	}
	
	@Test
	public void testCummaxRowVectorDenseSP() 
	{
		runColAggregateOperationTest(InputType.ROW_VECTOR, false, ExecType.SPARK);
	}
	
	@Test
	public void testCummaxRowVectorDenseNoRewritesSP() 
	{
		runColAggregateOperationTest(InputType.ROW_VECTOR, false, ExecType.SPARK, false);
	}
	
	@Test
	public void testCummaxMatrixDenseSP() 
	{
		runColAggregateOperationTest(InputType.MATRIX, false, ExecType.SPARK);
	}
	
	@Test
	public void testCummaxColVectorSparseSP() 
	{
		runColAggregateOperationTest(InputType.COL_VECTOR, true, ExecType.SPARK);
	}
	
	@Test
	public void testCummaxRowVectorSparseSP() 
	{
		runColAggregateOperationTest(InputType.ROW_VECTOR, true, ExecType.SPARK);
	}
	
	@Test
	public void testCummaxRowVectorSparseNoRewritesSP() 
	{
		runColAggregateOperationTest(InputType.ROW_VECTOR, true, ExecType.SPARK, false);
	}
	
	@Test
	public void testCummaxMatrixSparseSP() 
	{
		runColAggregateOperationTest(InputType.MATRIX, true, ExecType.SPARK);
	}

	
	/**
	 * 
	 * @param type
	 * @param sparse
	 * @param instType
	 */
	private void runColAggregateOperationTest( InputType type, boolean sparse, ExecType instType)
	{
		//by default we apply algebraic simplification rewrites
		runColAggregateOperationTest(type, sparse, instType, true);
	}
	
	/**
	 * 
	 * @param sparseM1
	 * @param sparseM2
	 * @param instType
	 */
	private void runColAggregateOperationTest( InputType type, boolean sparse, ExecType instType, boolean rewrites)
	{
		ExecMode platformOld = rtplatform;
		switch( instType ){
			case SPARK: rtplatform = ExecMode.SPARK; break;
			default: rtplatform = ExecMode.HYBRID; break;
		}
	
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		if( rtplatform == ExecMode.SPARK )
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;
		
		//rewrites
		boolean oldFlagRewrites = OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION;
		OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = rewrites;
		
		try
		{
			int cols = (type==InputType.COL_VECTOR) ? 1 : colsMatrix;
			int rows = (type==InputType.ROW_VECTOR) ? 1 : rowsMatrix;
			double sparsity = (sparse) ? spSparse : spDense;
			
			String TEST_CACHE_DIR = "";
			if (TEST_CACHE_ENABLED)
			{
				TEST_CACHE_DIR = type.ordinal() + "_" + sparsity + "/";
			}
			
			TestConfiguration config = getTestConfiguration(TEST_NAME);
			loadTestConfiguration(config, TEST_CACHE_DIR);
			
			// This is for running the junit test the new way, i.e., construct the arguments directly
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-explain", "-args", input("A"), output("B") };
			
			fullRScriptName = HOME + TEST_NAME + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " + expectedDir();
	
			//generate actual dataset 
			double[][] A = getRandomMatrix(rows, cols, -0.05, 1, sparsity, 7); 
			writeInputMatrixWithMTD("A", A, true);
	
			runTest(true, false, null, -1); 
			if( instType==ExecType.CP || instType==ExecType.SPARK ) //in CP no MR jobs should be executed
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
			OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = oldFlagRewrites;
		}
	}	
}