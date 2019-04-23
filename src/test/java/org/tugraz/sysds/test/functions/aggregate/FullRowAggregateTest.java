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

/**
 * NOTES:
 *  * the R package Matrix does not support colMins and colMaxs; hence, we use the matrixStats package 
 * 
 */
public class FullRowAggregateTest extends AutomatedTestBase 
{
	private final static String TEST_NAME1 = "RowSums";
	private final static String TEST_NAME2 = "RowMeans";
	private final static String TEST_NAME3 = "RowMaxs";
	private final static String TEST_NAME4 = "RowMins";
	private final static String TEST_NAME5 = "RowIndexMaxs";
	private final static String TEST_NAME6 = "RowIndexMins";
	
	private final static String TEST_DIR = "functions/aggregate/";
	private static final String TEST_CLASS_DIR = TEST_DIR + FullRowAggregateTest.class.getSimpleName() + "/";
	private final static double eps = 1e-10;
	
	private final static int rows1 = 1;
	private final static int rows2 = 1079;
	private final static int cols = 1005;
	private final static double sparsity1 = 0.1;
	private final static double sparsity2 = 0.7;
	
	private enum OpType{
		ROW_SUMS,
		ROW_MEANS,
		ROW_MAX,
		ROW_MIN,
		ROW_INDEXMAX,
		ROW_INDEXMIN
	}
	
	/**
	 * Main method for running one test at a time from Eclipse.
	 */
	public static void main(String[] args) {
		long startMsec = System.currentTimeMillis();

		FullRowAggregateTest t= new FullRowAggregateTest();
		t.setUpBase();
		t.setUp();
		
		t.setOutAndExpectedDeletionDisabled(true);
		
		t.tearDown();
		
		long elapsedMsec = System.currentTimeMillis() - startMsec;
		System.err.printf("Finished in %1.3f sec\n", elapsedMsec / 1000.0);
	
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
	public void testRowSumsDenseMatrixCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_SUMS, false, false, ExecType.CP);
	}
	
	@Test
	public void testRowMeansDenseMatrixCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MEANS, false, false, ExecType.CP);
	}	
	
	@Test
	public void testRowMaxDenseMatrixCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MAX, false, false, ExecType.CP);
	}
	
	@Test
	public void testRowMinDenseMatrixCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MIN, false, false, ExecType.CP);
	}
	
	@Test
	public void testRowIndexMaxDenseMatrixCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, false, false, ExecType.CP);
	}
	
	@Test
	public void testRowIndexMinDenseMatrixCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, false, false, ExecType.CP);
	}
	
	@Test
	public void testRowSumsDenseVectorCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_SUMS, false, true, ExecType.CP);
	}
	
	@Test
	public void testRowMeansDenseVectorCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MEANS, false, true, ExecType.CP);
	}	
	
	@Test
	public void testRowMaxDenseVectorCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MAX, false, true, ExecType.CP);
	}
	
	@Test
	public void testRowMinDenseVectorCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MIN, false, true, ExecType.CP);
	}
	
	@Test
	public void testRowIndexMaxDenseVectorCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, false, true, ExecType.CP);
	}
	
	@Test
	public void testRowIndexMinDenseVectorCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, false, true, ExecType.CP);
	}
	
	@Test
	public void testRowSumsSparseMatrixCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_SUMS, true, false, ExecType.CP);
	}
	
	@Test
	public void testRowMeansSparseMatrixCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MEANS, true, false, ExecType.CP);
	}	
	
	@Test
	public void testRowMaxSparseMatrixCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MAX, true, false, ExecType.CP);
	}
	
	@Test
	public void testRowMinSparseMatrixCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MIN, true, false, ExecType.CP);
	}
	
	@Test
	public void testRowIndexMaxSparseMatrixCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, true, false, ExecType.CP);
	}
	
	@Test
	public void testRowIndexMinparseMatrixCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, true, false, ExecType.CP);
	}
	
	@Test
	public void testRowSumsSparseVectorCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_SUMS, true, true, ExecType.CP);
	}
	
	@Test
	public void testRowMeansSparseVectorCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MEANS, true, true, ExecType.CP);
	}	
	
	@Test
	public void testRowMaxSparseVectorCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MAX, true, true, ExecType.CP);
	}
	
	@Test
	public void testRowMinSparseVectorCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MIN, true, true, ExecType.CP);
	}
	
	@Test
	public void testRowIndexMaxSparseVectorCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, true, true, ExecType.CP);
	}
	
	@Test
	public void testRowIndexMinSparseVectorCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, true, true, ExecType.CP);
	}
	
	
	@Test
	public void testRowIndexMaxDenseMatrixNegCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, false, false, ExecType.CP, true);
	}
	
	@Test
	public void testRowIndexMaxDenseVectorNegCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, false, true, ExecType.CP, true);
	}
	
	@Test
	public void testRowIndexMaxSparseMatrixNegCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, true, false, ExecType.CP, true);
	}
	
	@Test
	public void testRowIndexMaxSparseVectorNegCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, true, true, ExecType.CP, true);
	}
	
	
	@Test
	public void testRowIndexMinDenseMatrixNegCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, false, false, ExecType.CP, true);
	}
	
	@Test
	public void testRowIndexMinDenseVectorNegCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, false, true, ExecType.CP, true);
	}
	
	@Test
	public void testRowIndexMinSparseMatrixNegCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, true, false, ExecType.CP, true);
	}
	
	@Test
	public void testRowIndexMinSparseVectorNegCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, true, true, ExecType.CP, true);
	}
	
	
	@Test
	public void testRowSumsDenseMatrixNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_SUMS, false, false, ExecType.CP, false, false );
	}
	
	@Test
	public void testRowMeansDenseMatrixNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MEANS, false, false, ExecType.CP, false, false);
	}	
	
	@Test
	public void testRowMaxDenseMatrixNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MAX, false, false, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowMinDenseMatrixNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MIN, false, false, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowIndexMaxDenseMatrixNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, false, false, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowIndexMinDenseMatrixNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, false, false, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowSumsDenseVectorNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_SUMS, false, true, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowMeansDenseVectorNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MEANS, false, true, ExecType.CP, false, false);
	}	
	
	@Test
	public void testRowMaxDenseVectorNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MAX, false, true, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowMinDenseVectorNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MIN, false, true, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowIndexMaxDenseVectorNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, false, true, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowIndexMinDenseVectorNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, false, true, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowSumsSparseMatrixNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_SUMS, true, false, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowMeansSparseMatrixNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MEANS, true, false, ExecType.CP, false, false);
	}	
	
	@Test
	public void testRowMaxSparseMatrixNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MAX, true, false, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowMinSparseMatrixNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MIN, true, false, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowIndexMaxSparseMatrixNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, true, false, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowIndexMinparseMatrixNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, true, false, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowSumsSparseVectorNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_SUMS, true, true, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowMeansSparseVectorNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MEANS, true, true, ExecType.CP, false, false);
	}	
	
	@Test
	public void testRowMaxSparseVectorNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MAX, true, true, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowMinSparseVectorNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MIN, true, true, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowIndexMaxSparseVectorNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, true, true, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowIndexMinSparseVectorNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, true, true, ExecType.CP, false, false);
	}
	
	@Test
	public void testRowSumsDenseMatrixNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_SUMS, false, false, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowMeansDenseMatrixNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MEANS, false, false, ExecType.SPARK, false, false);
	}	
	
	@Test
	public void testRowMaxDenseMatrixNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MAX, false, false, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowMinDenseMatrixNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MIN, false, false, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowIndexMaxDenseMatrixNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, false, false, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowIndexMinDenseMatrixNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, false, false, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowSumsDenseVectorNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_SUMS, false, true, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowMeansDenseVectorNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MEANS, false, true, ExecType.SPARK, false, false);
	}	
	
	@Test
	public void testRowMaxDenseVectorNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MAX, false, true, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowMinDenseVectorNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MIN, false, true, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowIndexMaxDenseVectorNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, false, true, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowIndexMinDenseVectorNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, false, true, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowSumsSparseMatrixNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_SUMS, true, false, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowMeansSparseMatrixNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MEANS, true, false, ExecType.SPARK, false, false);
	}	
	
	@Test
	public void testRowMaxSparseMatrixNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MAX, true, false, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowMinSparseMatrixNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MIN, true, false, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowIndexMaxSparseMatrixNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, true, false, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowIndexMinSparseMatrixNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, true, false, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowSumsSparseVectorNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_SUMS, true, true, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowMeansSparseVectorNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MEANS, true, true, ExecType.SPARK, false, false);
	}	
	
	@Test
	public void testRowMaxSparseVectorNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MAX, true, true, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowMinSparseVectorNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_MIN, true, true, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowIndexMaxSparseVectorNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, true, true, ExecType.SPARK, false, false);
	}
	
	@Test
	public void testRowIndexMinSparseVectorNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, true, true, ExecType.SPARK, false, false);
	}

	//additional testcases for rowindexmax/rowindexmin with special data
	
	@Test
	public void testRowIndexMaxDenseMatrixNegNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, false, false, ExecType.CP, true, false);
	}
	
	@Test
	public void testRowIndexMaxDenseVectorNegNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, false, true, ExecType.CP, true, false);
	}
	
	@Test
	public void testRowIndexMaxSparseMatrixNegNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, true, false, ExecType.CP, true, false);
	}
	
	@Test
	public void testRowIndexMaxSparseVectorNegNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, true, true, ExecType.CP, true, false);
	}
	
	@Test
	public void testRowIndexMinDenseMatrixPosNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, false, false, ExecType.CP, true, false);
	}
	
	@Test
	public void testRowIndexMinDenseVectorPosNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, false, true, ExecType.CP, true, false);
	}
	
	@Test
	public void testRowIndexMinSparseMatrixPosNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, true, false, ExecType.CP, true, false);
	}
	
	@Test
	public void testRowIndexMinSparseVectorPosNoRewritesCP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, true, true, ExecType.CP, true, false);
	}
	
	
	@Test
	public void testRowIndexMaxDenseMatrixNegNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, false, false, ExecType.SPARK, true, false);
	}
	
	@Test
	public void testRowIndexMaxDenseVectorNegNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, false, true, ExecType.SPARK, true, false);
	}
	
	@Test
	public void testRowIndexMaxSparseMatrixNegNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, true, false, ExecType.SPARK, true, false);
	}
	
	@Test
	public void testRowIndexMaxSparseVectorNegNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMAX, true, true, ExecType.SPARK, true, false);
	}
	
	@Test
	public void testRowIndexMinDenseMatrixPosNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, false, false, ExecType.SPARK, true, false);
	}
	
	@Test
	public void testRowIndexMinDenseVectorPosNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, false, true, ExecType.SPARK, true, false);
	}
	
	@Test
	public void testRowIndexMinSparseMatrixPosNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, true, false, ExecType.SPARK, true, false);
	}
	
	@Test
	public void testRowIndexMinSparseVectorPosNoRewritesSP() 
	{
		runRowAggregateOperationTest(OpType.ROW_INDEXMIN, true, true, ExecType.SPARK, true, false);
	}
	
	/**
	 * 
	 * @param type
	 * @param sparse
	 * @param vector
	 * @param instType
	 */
	private void runRowAggregateOperationTest( OpType type, boolean sparse, boolean vector, ExecType instType)
	{
		runRowAggregateOperationTest(type, sparse, vector, instType, false); //by default no special data
	}
	
	/**
	 * 
	 * @param type
	 * @param sparse
	 * @param vector
	 * @param instType
	 * @param specialData
	 */
	private void runRowAggregateOperationTest( OpType type, boolean sparse, boolean vector, ExecType instType, boolean specialData)
	{
		runRowAggregateOperationTest(type, sparse, vector, instType, specialData, true); //by default apply algebraic simplification
	}
	
	/**
	 * 
	 * @param sparseM1
	 * @param sparseM2
	 * @param instType
	 */
	private void runRowAggregateOperationTest( OpType type, boolean sparse, boolean vector, ExecType instType, boolean specialData, boolean rewrites)
	{
		ExecMode platformOld = rtplatform;
		switch( instType ){
			case SPARK: rtplatform = ExecMode.SPARK; break;
			default: rtplatform = ExecMode.HYBRID; break;
		}
	
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		if( rtplatform == ExecMode.SPARK )
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;

		boolean oldRewritesFlag = OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION;
		OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = rewrites;
		
		try
		{
			String TEST_NAME = null;
			switch( type )
			{
				case ROW_SUMS: TEST_NAME = TEST_NAME1; break;
				case ROW_MEANS: TEST_NAME = TEST_NAME2; break;
				case ROW_MAX: TEST_NAME = TEST_NAME3; break;
				case ROW_MIN: TEST_NAME = TEST_NAME4; break;
				case ROW_INDEXMAX: TEST_NAME = TEST_NAME5; break;
				case ROW_INDEXMIN: TEST_NAME = TEST_NAME6; break;
			}
			
			int rows = (vector) ? rows1 : rows2;
			double sparsity = (sparse) ? sparsity1 : sparsity2;
			
			TestConfiguration config = getTestConfiguration(TEST_NAME);
	
			//generate actual dataset
			double min, max;
			
			// In case of ROW_INDEXMAX, generate all negative data 
			// so that the value 0 is the maximum value. Similarly,
			// in case of ROW_INDEXMIN, generate all positive data.
			if ( type == OpType.ROW_INDEXMAX ) {
				//special data: negative, 0 is actual max
				min = specialData ? -1 : -0.05;
				max = specialData ? -0.05 : 1;
			}
			else if (type == OpType.ROW_INDEXMIN ){
				//special data: positive, 0 is actual min
				min = specialData ? 0.05 : -1;
				max = specialData ? 1 : 0.05;
			} else {
				min = -0.05;
				max = 1;
			}
			
			String TEST_CACHE_DIR = "";
			if (TEST_CACHE_ENABLED)
			{
				TEST_CACHE_DIR = type.ordinal() + "_" + rows + "_" + specialData + "_" + sparsity + "/";
			}
			
			loadTestConfiguration(config, TEST_CACHE_DIR);
			
			/* This is for running the junit test the new way, i.e., construct the arguments directly */
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-explain", "-args", input("A"),
				Integer.toString(rows), Integer.toString(cols), output("B") };
			
			fullRScriptName = HOME + TEST_NAME + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " + expectedDir();
					
			double[][] A = getRandomMatrix(rows, cols, min, max, sparsity, 7); 
			writeInputMatrix("A", A, true);
	
			boolean exceptionExpected = false;
			runTest(true, exceptionExpected, null, -1); 
		
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
			OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = oldRewritesFlag;
		}
	}
	
}