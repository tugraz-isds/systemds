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

package org.tugraz.sysds.test.functions.binary.matrix_full_cellwise;

import java.util.HashMap;

import org.junit.Test;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.runtime.meta.MatrixCharacteristics;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class FullSortedOuterCompareTest extends AutomatedTestBase 
{
	private final static String TEST_NAME = "FullSortedOuterCompare";
	private final static String TEST_DIR = "functions/binary/matrix_full_cellwise/";
	private final static String TEST_CLASS_DIR = TEST_DIR + FullSortedOuterCompareTest.class.getSimpleName() + "/";
	private final static double eps = 1e-10;
	private final static int rows1 = 1111;
	
	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[]{"C"}));
	}

	@Test
	public void testLessIncreasingCP() {
		runMatrixVectorCellwiseOperationTest("<", true, ExecType.CP);
	}
	
	@Test
	public void testLessEqualsIncreasingCP() {
		runMatrixVectorCellwiseOperationTest("<=", true, ExecType.CP);
	}
	
	@Test
	public void testGreaterIncreasingCP() {
		runMatrixVectorCellwiseOperationTest(">", true, ExecType.CP);
	}
	
	@Test
	public void testGreaterEqualsIncreasingCP() {
		runMatrixVectorCellwiseOperationTest(">=", true, ExecType.CP);
	}
	
	@Test
	public void testEqualsIncreasingCP() {
		runMatrixVectorCellwiseOperationTest("==", true, ExecType.CP);
	}
	
	@Test
	public void testNotEqualsIncreasingCP() {
		runMatrixVectorCellwiseOperationTest("!=", true, ExecType.CP);
	}

	@Test
	public void testLessIncreasingSP() {
		runMatrixVectorCellwiseOperationTest("<", true, ExecType.SPARK);
	}
	
	@Test
	public void testLessEqualsIncreasingSP() {
		runMatrixVectorCellwiseOperationTest("<=", true, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterIncreasingSP() {
		runMatrixVectorCellwiseOperationTest(">", true, ExecType.SPARK);
	}
	
	@Test
	public void testGreaterEqualsIncreasingSP() {
		runMatrixVectorCellwiseOperationTest(">=", true, ExecType.SPARK);
	}
	
	@Test
	public void testEqualsIncreasingSP() {
		runMatrixVectorCellwiseOperationTest("==", true, ExecType.SPARK);
	}
	
	@Test
	public void testNotEqualsIncreasingSP() {
		runMatrixVectorCellwiseOperationTest("!=", true, ExecType.SPARK);
	}
	
	private void runMatrixVectorCellwiseOperationTest( String otype, boolean incr, ExecType et)
	{
		//rtplatform for MR
		ExecMode platformOld = rtplatform;
		switch( et ){
			case SPARK: rtplatform = ExecMode.SPARK; break;
			default: rtplatform = ExecMode.HYBRID; break;
		}
	
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		if( rtplatform == ExecMode.SPARK )
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;
	
		try
		{
			loadTestConfiguration(getTestConfiguration(TEST_NAME));
			
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-explain", "recompile_runtime", "-args",
				String.valueOf(rows1), otype, output("C") };
			
			fullRScriptName = HOME + TEST_NAME + ".R";
			rCmd = "Rscript" + " " + fullRScriptName +" " + String.valueOf(rows1) 
				+ " " + getSafeOp(otype) + " " + expectedDir();
			
			runTest(true, false, null, -1); 
			runRScript(true); 
			
			//compare matrices 
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("C");
			HashMap<CellIndex, Double> rfile  = readRMatrixFromFS("C");
			TestUtils.compareMatrices(dmlfile, rfile, eps, "Stat-DML", "Stat-R");
			checkDMLMetaDataFile("C", new MatrixCharacteristics(rows1,rows1,1,1));
		}
		finally {
			rtplatform = platformOld;
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
		}
	}
	
	private String getSafeOp(String op) {
		switch(op) {
			case "<": return "lt";
			case "<=": return "lte";
			case ">": return "gt";
			case ">=": return "gte";
			default: return op;
		}
	}
}
