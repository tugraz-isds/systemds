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

import java.util.HashMap;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.lops.UAggOuterChain;
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.runtime.instructions.Instruction;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.runtime.meta.MatrixCharacteristics;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;
import org.tugraz.sysds.utils.Statistics;

/**
 * TODO: extend test by various binary operator - unary aggregate operator combinations.
 * 
 */
public class UaggOuterChainTest extends AutomatedTestBase 
{
	private final static String TEST_NAME1 = "UaggOuterChain";
	private final static String TEST_DIR = "functions/binary/matrix/";
	private final static String TEST_CLASS_DIR = TEST_DIR + UaggOuterChainTest.class.getSimpleName() + "/";
	private final static double eps = 1e-8;
	
	private final static int rows = 1468;
	private final static int cols1 = 73; //single block
	private final static int cols2 = 1052; //multi block
	
	private final static double sparsity1 = 0.5; //dense 
	private final static double sparsity2 = 0.1; //sparse
	
	public enum Type{
		GREATER,
		LESS,
		EQUALS,
		NOT_EQUALS,
		GREATER_EQUALS,
		LESS_EQUALS,
	}
	
	public enum SumType{ROW_SUM, COL_SUM, SUM_ALL, ROW_INDEX_MAX, ROW_INDEX_MIN}
		
	@Override
	public void setUp() 
	{
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME1, 
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] { "C" })); 

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
	
	// Less Uagg RowSums -- MR

	// -------------------------

	// Less Uagg RowSums -- SP
	@Test
	public void testLessUaggOuterChainRowSumsSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.LESS, true, false, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessUaggOuterChainRowSumsSingleSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS, true, true, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessUaggOuterChainRowSumsMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS, false, false, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessUaggOuterChainRowSumsMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS, false, true, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	// Greater Uagg RowSums -- SP
	@Test
	public void testGreaterUaggOuterChainRowSumsSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.GREATER, true, false, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterUaggOuterChainRowSumsSingleSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER, true, true, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterUaggOuterChainRowSumsMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER, false, false, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterUaggOuterChainRowSumsMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER, false, true, SumType.ROW_SUM, false, ExecType.SPARK);
	}

	
	// LessEquals Uagg RowSums -- SP
	@Test
	public void testLessEqualsUaggOuterChainRowSumsSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.LESS_EQUALS, true, false, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessEqualsUaggOuterChainRowSumsSingleSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS_EQUALS, true, true, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessEqualsUaggOuterChainRowSumsMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS_EQUALS, false, false, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessEqualsUaggOuterChainRowSumsMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS_EQUALS, false, true, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	
	// GreaterThanEquals Uagg RowSums -- SP
	@Test
	public void testGreaterEqualsUaggOuterChainRowSumsSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, true, false, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterEqualsUaggOuterChainRowSumsSingleSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, true, true, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterEqualsUaggOuterChainRowSumsMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, false, false, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterEqualsUaggOuterChainRowSumsMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, false, true, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	
	// Equals Uagg RowSums -- SP
	@Test
	public void testEqualsUaggOuterChainRowSumsSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.EQUALS, true, false, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testEqualsUaggOuterChainRowSumsSingleSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.EQUALS, true, true, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testEqualsUaggOuterChainRowSumsMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.EQUALS, false, false, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testEqualsUaggOuterChainRowSumsMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.EQUALS, false, true, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	

	// NotEquals Uagg RowSums -- SP
	@Test
	public void testNotEqualsUaggOuterChainRowSumsSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.NOT_EQUALS, true, false, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testNotEqualsUaggOuterChainRowSumsSingleSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.NOT_EQUALS, true, true, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testNotEqualsUaggOuterChainRowSumsMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.NOT_EQUALS, false, false, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testNotEqualsUaggOuterChainRowSumsMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.NOT_EQUALS, false, true, SumType.ROW_SUM, false, ExecType.SPARK);
	}
	
	
	
	// -------------------------
	// ColSums

	// Less Uagg ColSums -- SP
	@Test
	public void testLessUaggOuterChainColSumsSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.LESS, true, false, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessUaggOuterChainColSumsSingleSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS, true, true, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessUaggOuterChainColSumsMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS, false, false, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessUaggOuterChainColSumsMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS, false, true, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	// GreaterThanEquals Uagg ColSums -- SP
	@Test
	public void testGreaterEqualsUaggOuterChainColSumsSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, true, false, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterEqualsUaggOuterChainColSumsSingleSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, true, true, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterEqualsUaggOuterChainColSumsMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, false, false, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterEqualsUaggOuterChainColSumsMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, false, true, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	
	// Greater Uagg ColSums -- SP
	@Test
	public void testGreaterUaggOuterChainColSumsSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.GREATER, true, false, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterUaggOuterChainColSumsSingleSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER, true, true, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterUaggOuterChainColSumsMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER, false, false, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterUaggOuterChainColSumsMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER, false, true, SumType.COL_SUM, false, ExecType.SPARK);
	}

	
	// LessEquals Uagg ColSums -- SP
	@Test
	public void testLessEqualsUaggOuterChainColSumsSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.LESS_EQUALS, true, false, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessEqualsUaggOuterChainColSumsSingleSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS_EQUALS, true, true, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessEqualsUaggOuterChainColSumsMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS_EQUALS, false, false, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessEqualsUaggOuterChainColSumsMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS_EQUALS, false, true, SumType.COL_SUM, false, ExecType.SPARK);
	}
	

	// Equals Uagg ColSums -- SP
	@Test
	public void testEqualsUaggOuterChainColSumsSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.EQUALS, true, false, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testEqualsUaggOuterChainColSumsSingleSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.EQUALS, true, true, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testEqualsUaggOuterChainColSumsMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.EQUALS, false, false, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testEqualsUaggOuterChainColSumsMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.EQUALS, false, true, SumType.COL_SUM, false, ExecType.SPARK);
	}
	

	// NotEquals Uagg ColSums -- SP
	@Test
	public void testNotEqualsUaggOuterChainColSumsSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.NOT_EQUALS, true, false, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testNotEqualsUaggOuterChainColSumsSingleSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.NOT_EQUALS, true, true, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testNotEqualsUaggOuterChainColSumsMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.NOT_EQUALS, false, false, SumType.COL_SUM, false, ExecType.SPARK);
	}
	
	@Test
	public void testNotEqualsUaggOuterChainColSumsMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.NOT_EQUALS, false, true, SumType.COL_SUM, false, ExecType.SPARK);
	}
	

	// Empty Block (Data with 0.0 values)
	
	@Test
	public void testGreaterEqualsUaggOuterChainRowSumsEmptyMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, false, true, SumType.ROW_SUM, true, ExecType.SPARK);
	}
	
	@Test
	public void testNotEqualsUaggOuterChainColSumsEmptyMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.NOT_EQUALS, false, false, SumType.COL_SUM, true, ExecType.SPARK);
	}
	
	//---------------------------------------------------
	// Sums operation test cases.
	//---------------------------------------------------
	
	// Less Uagg Sums -- SP
	@Test
	public void testLessUaggOuterChainSumsSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.LESS, true, false, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessUaggOuterChainSumsSingleSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS, true, true, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessUaggOuterChainSumsMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS, false, false, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessUaggOuterChainSumsMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS, false, true, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	// GreaterThanEquals Uagg Sums -- SP
	@Test
	public void testGreaterEqualsUaggOuterChainSumsSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, true, false, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterEqualsUaggOuterChainSumsSingleSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, true, true, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterEqualsUaggOuterChainSumsMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, false, false, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterEqualsUaggOuterChainSumsMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, false, true, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	
	// Greater Uagg Sums -- SP
	@Test
	public void testGreaterUaggOuterChainSumsSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.GREATER, true, false, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterUaggOuterChainSumsSingleSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER, true, true, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterUaggOuterChainSumsMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER, false, false, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterUaggOuterChainSumsMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER, false, true, SumType.SUM_ALL, false, ExecType.SPARK);
	}

	
	// LessEquals Uagg Sums -- SP
	@Test
	public void testLessEqualsUaggOuterChainSumsSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.LESS_EQUALS, true, false, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessEqualsUaggOuterChainSumsSingleSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS_EQUALS, true, true, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessEqualsUaggOuterChainSumsMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS_EQUALS, false, false, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessEqualsUaggOuterChainSumsMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS_EQUALS, false, true, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	

	// Equals Uagg Sums -- SP
	@Test
	public void testEqualsUaggOuterChainSumsSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.EQUALS, true, false, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	@Test
	public void testEqualsUaggOuterChainSumsSingleSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.EQUALS, true, true, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	@Test
	public void testEqualsUaggOuterChainSumsMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.EQUALS, false, false, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	@Test
	public void testEqualsUaggOuterChainSumsMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.EQUALS, false, true, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	

	// NotEquals Uagg Sums -- SP
	@Test
	public void testNotEqualsUaggOuterChainSumsSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.NOT_EQUALS, true, false, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	@Test
	public void testNotEqualsUaggOuterChainSumsSingleSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.NOT_EQUALS, true, true, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	@Test
	public void testNotEqualsUaggOuterChainSumsMultiDenseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.NOT_EQUALS, false, false, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	@Test
	public void testNotEqualsUaggOuterChainSumsMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.NOT_EQUALS, false, true, SumType.SUM_ALL, false, ExecType.SPARK);
	}
	
	
	// Empty Block Sums All (Data with 0.0 values)
	
	@Test
	public void testGreaterEqualsUaggOuterChainSumsEmptyMultiSparseSP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, false, true, SumType.SUM_ALL, true, ExecType.SPARK);
	}
	
	// Uagg RowIndexMax -- SP

	@Test
	public void testEqualsUaggOuterChainRowIndexMaxSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.EQUALS, true, false, SumType.ROW_INDEX_MAX, false, ExecType.SPARK);
	}
	
	@Test
	public void testNotEqualsUaggOuterChainRowIndexMaxSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.NOT_EQUALS, true, false, SumType.ROW_INDEX_MAX, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessUaggOuterChainRowIndexMaxSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.LESS, true, false, SumType.ROW_INDEX_MAX, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessEqualsUaggOuterChainRowIndexMaxSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.LESS_EQUALS, true, false, SumType.ROW_INDEX_MAX, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterUaggOuterChainRowIndexMaxSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.GREATER, true, false, SumType.ROW_INDEX_MAX, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterEqualsUaggOuterChainRowIndexMaxSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, true, false, SumType.ROW_INDEX_MAX, false, ExecType.SPARK);
	}
	

	// Uagg RowIndexMax -- MR

	
	// Uagg RowIndexMin -- SP

	@Test
	public void testEqualsUaggOuterChainRowIndexMinSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.EQUALS, true, false, SumType.ROW_INDEX_MIN, false, ExecType.SPARK);
	}
	
	@Test
	public void testLessUaggOuterChainRowIndexMinSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.LESS, true, false, SumType.ROW_INDEX_MIN, false, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterEqualsUaggOuterChainRowIndexMinSingleDenseSP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, true, false, SumType.ROW_INDEX_MIN, false, ExecType.SPARK);
	}
	
	
	// CP Test cases.

	@Test
	public void testLessUaggOuterChainRowSumsMultiDenseCP() 
	{
		runBinUaggTest(TEST_NAME1, Type.LESS, false, false, SumType.ROW_SUM, false, ExecType.CP);
	}
	
	@Test
	public void testGreaterEqualsUaggOuterChainColSumsSingleSparseCP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, true, true, SumType.COL_SUM, false, ExecType.CP);
	}
	
	@Test
	public void testGreaterEqualsUaggOuterChainRowIndexMaxSingleDenseCP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, true, false, SumType.ROW_INDEX_MAX, false, ExecType.CP);
	}
	

	@Test
	public void testGreaterEqualsUaggOuterChainRowIndexMinSingleDenseCP() 
	{
		 runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, true, false, SumType.ROW_INDEX_MIN, false, ExecType.CP);
	}
	
	@Test
	public void testGreaterEqualsUaggOuterChainSumsEmptyMultiSparseCP() 
	{
		runBinUaggTest(TEST_NAME1, Type.GREATER_EQUALS, false, true, SumType.SUM_ALL, true, ExecType.CP);
	}
	


	/**
	 * 
	 * @param sparseM1
	 * @param sparseM2
	 * @param instType
	 */
	private void runBinUaggTest( String testname, Type type, boolean singleBlock, boolean sparse, SumType sumType, boolean bEmptyBlock, ExecType instType)
	{
		//rtplatform for MR
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
			String TEST_NAME = testname;
			TestConfiguration config = getTestConfiguration(TEST_NAME);
			
			/* This is for running the junit test the new way, i.e., construct the arguments directly */

			String suffix = "";
			
			switch (type) {
				case GREATER:
					suffix = "Greater";
					break;
				case LESS:
					suffix = "";
					break;
				case EQUALS:
					suffix = "Equals";
					break;
				case NOT_EQUALS:
					suffix = "NotEquals";
					break;
				case GREATER_EQUALS:
					suffix = "GreaterEquals";
					break;
				case LESS_EQUALS:
					suffix = "LessEquals";
					break;
			}

			String strSumTypeSuffix = null; 
			switch (sumType) {
				case ROW_SUM:
					strSumTypeSuffix = new String("");
					break;
				case COL_SUM:
					strSumTypeSuffix = new String("ColSums");
					break;
				case SUM_ALL:
					strSumTypeSuffix = new String("Sums");
					break;
				case ROW_INDEX_MAX:
					strSumTypeSuffix = new String("RowIndexMax");
					break;
				case ROW_INDEX_MIN:
					strSumTypeSuffix = new String("RowIndexMin");
					break;
			}
	
			double dAMinVal = -1, dAMaxVal = 1, dBMinVal = -1, dBMaxVal = 1;
			
			if(bEmptyBlock)
				if(sumType == SumType.ROW_SUM) {
					dAMinVal = 0;
					dAMaxVal = 0;
				} else {
					dBMinVal = 0;
					dBMaxVal = 0;
				}
			
			double sparsity = sparse?sparsity2:sparsity1;
			
			String TEST_CACHE_DIR = "";
			if (TEST_CACHE_ENABLED)
			{
				TEST_CACHE_DIR = type.ordinal() + "_"+ sumType.ordinal() + "_" +
						singleBlock + "_" + sparsity + "_" + bEmptyBlock + "/";
			}
			
			loadTestConfiguration(config, TEST_CACHE_DIR);
			
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + suffix + strSumTypeSuffix + ".dml";
			programArgs = new String[]{"-stats", "-explain","-args", 
				input("A"), input("B"), output("C")};
			
			fullRScriptName = HOME + TEST_NAME + suffix + strSumTypeSuffix +".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " + expectedDir();
			
			//generate actual datasets
			double[][] A = getRandomMatrix(rows, 1, dAMinVal, dAMaxVal, sparsity, 235);
			writeInputMatrixWithMTD("A", A, true);
			double[][] B = getRandomMatrix(1, singleBlock?cols1:cols2, dBMinVal, dBMaxVal, sparsity, 124);
			writeInputMatrixWithMTD("B", B, true);
			
			runTest(true, false, null, -1); 
			runRScript(true); 
			
			//compare matrices 
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("C");
			HashMap<CellIndex, Double> rfile  = readRMatrixFromFS("C");
			TestUtils.compareMatrices(dmlfile, rfile, eps, "Stat-DML", "Stat-R");
			
			if(sumType == SumType.ROW_SUM)
				checkDMLMetaDataFile("C", new MatrixCharacteristics(rows,1,1,1)); //rowsums
			else if(sumType == SumType.COL_SUM)
				checkDMLMetaDataFile("C", new MatrixCharacteristics(1,singleBlock?cols1:cols2,1,1)); //colsums
			if(sumType == SumType.SUM_ALL)
				checkDMLMetaDataFile("C", new MatrixCharacteristics(1,1,1,1)); //sums
			
			//check compiled/executed jobs
			if( rtplatform != ExecMode.SPARK && instType != ExecType.CP) {
				int expectedNumCompiled = 2; //reblock+gmr if uaggouterchain; otherwise 3
				if(sumType == SumType.SUM_ALL)
					expectedNumCompiled = 3;  // scaler to matrix conversion.
				int expectedNumExecuted = expectedNumCompiled;
				checkNumCompiledSparkInst(expectedNumCompiled);
				checkNumExecutedSparkInst(expectedNumExecuted);
			}
			
			//check statistics for right operator in cp and spark
			if( instType == ExecType.CP ) {
				Assert.assertTrue("Missing opcode sp_uaggouterchain", Statistics.getCPHeavyHitterOpCodes().contains(UAggOuterChain.OPCODE));
			}
			else if( instType == ExecType.SPARK ) {
				Assert.assertTrue("Missing opcode sp_uaggouterchain",
						Statistics.getCPHeavyHitterOpCodes().contains(Instruction.SP_INST_PREFIX+UAggOuterChain.OPCODE));
			}
		}
		finally
		{
			rtplatform = platformOld;
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
		}
	}

}