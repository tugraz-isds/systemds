/*
 * Copyright 2019 Graz University of Technology
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tugraz.sysds.test.integration.applications;

import org.junit.runners.Parameterized.Parameters;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestUtils;

import java.util.*;

public abstract class LineageTraceTest extends AutomatedTestBase {
	
	protected final static String TEST_DIR = "applications/lineage_trace/";
	protected final static String TEST_NAME = "LineageTrace";
	protected String TEST_CLASS_DIR = TEST_DIR + LineageTraceTest.class.getSimpleName() + "/";
	
	protected int numRecords, numFeatures;
	
	public LineageTraceTest(int rows, int cols) {
		numRecords = rows;
		numFeatures = cols;
	}
	
	@Parameters
	public static Collection<Object[]> data() {
		Object[][] data = new Object[][]{
				{10, 5} //, {1000, 500}
		};
		return Arrays.asList(data);
	}
	
	@Override
	public void setUp() {
		addTestConfiguration(TEST_CLASS_DIR, TEST_NAME);
	}
	
	protected void testLineageTrace() {
		boolean old_simplification = OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION;
		boolean old_sum_product = OptimizerUtils.ALLOW_SUM_PRODUCT_REWRITES;
		
		try {
			System.out.println("------------ BEGIN " + TEST_NAME + "------------");
			
			OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = false;
			OptimizerUtils.ALLOW_SUM_PRODUCT_REWRITES = false;
			
			int rows = numRecords;
			int cols = numFeatures;
			
			getAndLoadTestConfiguration(TEST_NAME);

//        rtplatform = Types.ExecMode.SPARK;
//        DMLScript.USE_LOCAL_SPARK_CONFIG = true;
			
			List<String> proArgs = new ArrayList<String>();
			
			proArgs.add("-stats");
//        proArgs.add("-explain");
//        proArgs.add("hops");
			proArgs.add("-lineage");
			proArgs.add("-args");
			proArgs.add(input("X"));
			proArgs.add(output("X"));
			proArgs.add(output("Y"));
			programArgs = proArgs.toArray(new String[proArgs.size()]);
			
			fullDMLScriptName = getScript();
			
			double[][] X = getRandomMatrix(rows, cols, 0, 1, 0.8, -1);
			writeInputMatrixWithMTD("X", X, true);
			
			String expected_X_lineage =
					"(0) target/testTemp/applications/lineage_trace/LineageTraceDMLTest/in/X\n" +
							"(1) false\n" +
							"(2) createvar (0) (1)\n" +
							"(6) rblk (2)\n" +
							"(10) 3\n" +
							"(11) * (6) (10)\n" +
							"(15) 5\n" +
							"(16) + (11) (15)\n";
			String expected_Y_lineage =
					expected_X_lineage + "(20) tsmm (16)\n";
			
			runTest(true, EXCEPTION_NOT_EXPECTED, null, -1);
			
			HashMap<CellIndex, Double> X_DML = readDMLMatrixFromHDFS("X");
			HashMap<CellIndex, Double> Y_DML = readDMLMatrixFromHDFS("Y");
			
			String X_lineage = readDMLLineageFromHDFS("X");
			String Y_lineage = readDMLLineageFromHDFS("Y");
			
			TestUtils.compareScalars(expected_X_lineage, X_lineage);
			TestUtils.compareScalars(expected_Y_lineage, Y_lineage);
		} finally {
			OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = old_simplification;
			OptimizerUtils.ALLOW_SUM_PRODUCT_REWRITES = old_sum_product;
		}
	}
}
