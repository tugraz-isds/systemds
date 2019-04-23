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
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class QuantileWeightsTest extends AutomatedTestBase 
{
	private final static String TEST_NAME1 = "QuantileWeights";
	private final static String TEST_NAME2 = "MedianWeights";
	private final static String TEST_NAME3 = "IQMWeights";
	
	private final static String TEST_DIR = "functions/ternary/";
	private final static String TEST_CLASS_DIR = TEST_DIR + QuantileWeightsTest.class.getSimpleName() + "/";
	private final static double eps = 1e-10;
	
	private final static int rows = 1973;
	private final static int maxVal = 7; 
	private final static double sparsity1 = 0.9;
	private final static double sparsity2 = 0.3;
	
	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] { "R" }) );
		addTestConfiguration(TEST_NAME2, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2, new String[] { "R" }) );
		addTestConfiguration(TEST_NAME3, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME3, new String[] { "R" }) );
	}
	
	@Test
	public void testQuantile1DenseCP() {
		runQuantileTest(TEST_NAME1, 0.25, false, ExecType.CP);
	}
	
	@Test
	public void testQuantile2DenseCP() {
		runQuantileTest(TEST_NAME1, 0.50, false, ExecType.CP);
	}
	
	@Test
	public void testQuantile3DenseCP() {
		runQuantileTest(TEST_NAME1, 0.75, false, ExecType.CP);
	}
	
	@Test
	public void testQuantile1SparseCP() {
		runQuantileTest(TEST_NAME1, 0.25, true, ExecType.CP);
	}
	
	@Test
	public void testQuantile2SparseCP() {
		runQuantileTest(TEST_NAME1, 0.50, true, ExecType.CP);
	}
	
	@Test
	public void testQuantile3SparseCP() {
		runQuantileTest(TEST_NAME1, 0.75, true, ExecType.CP);
	}
	
	@Test
	public void testQuantile1DenseSP() {
		runQuantileTest(TEST_NAME1, 0.25, false, ExecType.SPARK);
	}
	
	@Test
	public void testQuantile2DenseSP() {
		runQuantileTest(TEST_NAME1, 0.50, false, ExecType.SPARK);
	}
	
	@Test
	public void testQuantile3DenseSP() {
		runQuantileTest(TEST_NAME1, 0.75, false, ExecType.SPARK);
	}
	
	@Test
	public void testQuantile1SparseSP() {
		runQuantileTest(TEST_NAME1, 0.25, true, ExecType.SPARK);
	}
	
	@Test
	public void testQuantile2SparseSP() {
		runQuantileTest(TEST_NAME1, 0.50, true, ExecType.SPARK);
	}
	
	@Test
	public void testQuantile3SparseSP() {
		runQuantileTest(TEST_NAME1, 0.75, true, ExecType.SPARK);
	}

	@Test
	public void testMedianDenseCP() {
		runQuantileTest(TEST_NAME2, -1, false, ExecType.CP);
	}
	
	@Test
	public void testMedianSparseCP() {
		runQuantileTest(TEST_NAME2, -1, true, ExecType.CP);
	}
	
	@Test
	public void testMedianDenseSP() {
		runQuantileTest(TEST_NAME2, -1, false, ExecType.SPARK);
	}

	@Test
	public void testMedianSparseSP() {
		runQuantileTest(TEST_NAME2, -1, true, ExecType.SPARK);
	}

	@Test
	public void testIQMDenseCP() {
		runQuantileTest(TEST_NAME3, -1, false, ExecType.CP);
	}
	
	@Test
	public void testIQMSparseCP() {
		runQuantileTest(TEST_NAME3, -1, true, ExecType.CP);
	}
	
	@Test
	public void testIQMDenseSP() {
		runQuantileTest(TEST_NAME3, -1, false, ExecType.SPARK);
	}

	@Test
	public void testIQMSparseSP() {
		runQuantileTest(TEST_NAME3, -1, true, ExecType.SPARK);
	}
	
	private void runQuantileTest( String TEST_NAME, double p, boolean sparse, ExecType et)
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
			TestConfiguration config = getTestConfiguration(TEST_NAME);
			loadTestConfiguration(config);
			
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-args", 
				input("A"), input("W"), Double.toString(p), output("R")};
			fullRScriptName = HOME + TEST_NAME + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + 
				inputDir() + " " + p + " " + expectedDir();
			
			//generate actual dataset (always dense because values <=0 invalid)
			double sparsitya = sparse ? sparsity2 : sparsity1;
			double[][] A = getRandomMatrix(rows, 1, 1, maxVal, sparsitya, 1236); 
			writeInputMatrixWithMTD("A", A, true);
			double[][] W = getRandomMatrix(rows, 1, 1, 1, 1.0, 1); 
			writeInputMatrixWithMTD("W", W, true);
			
			runTest(true, false, null, -1); 
			runRScript(true); 
			
			//compare matrices 
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("R");
			HashMap<CellIndex, Double> rfile  = readRMatrixFromFS("R");
			TestUtils.compareMatrices(dmlfile, rfile, eps, "Stat-DML", "Stat-R");
		}
		finally {
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
			rtplatform = platformOld;
		}
	}
}
