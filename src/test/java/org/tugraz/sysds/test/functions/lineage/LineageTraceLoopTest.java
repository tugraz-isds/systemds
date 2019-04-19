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

package org.tugraz.sysds.test.functions.lineage;

import org.junit.Test;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.runtime.lineage.LineageItem;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestUtils;

import java.util.ArrayList;
import java.util.List;

public class LineageTraceLoopTest extends AutomatedTestBase {
	
	protected static final String TEST_DIR = "functions/lineage/";
	protected static final String TEST_NAME = "LineageTraceLoop";
	protected String TEST_CLASS_DIR = TEST_DIR + LineageTraceLoopTest.class.getSimpleName() + "/";
	
	protected static final int numRecords = 10;
	protected static final int numFeatures = 5;
	
	public LineageTraceLoopTest() {
		
	}
	
	@Override
	public void setUp() {
		addTestConfiguration(TEST_CLASS_DIR, TEST_NAME);
	}
	
	@Test
	public void testLineageTraceLoop() {
		boolean old_simplification = OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION;
		boolean old_sum_product = OptimizerUtils.ALLOW_SUM_PRODUCT_REWRITES;
		
		try {
			System.out.println("------------ BEGIN " + TEST_NAME + "------------");
			
			OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = false;
			OptimizerUtils.ALLOW_SUM_PRODUCT_REWRITES = false;
			
			int rows = numRecords;
			int cols = numFeatures;
			
			getAndLoadTestConfiguration(TEST_NAME);
			
			List<String> proArgs = new ArrayList<String>();
			
			proArgs.add("-stats");
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
					"(0) target/testTemp/functions/lineage/LineageTraceSelfReferenceTest/in/X\n" +
							"(1) false\n" +
							"(2) createvar (0) (1)\n" +
							"(6) rblk (2)\n" +
							"(9) cpvar (6)\n" +
							"(21) 7\n" +
							"(22) + (9) (21)\n" +
							"(23) cpvar (22)\n" +
							"(38) * (23) (23)\n" +
							"(42) * (38) (23)\n" +
							"(43) cpvar (42)\n" +
							"(58) 7\n" +
							"(59) + (43) (58)\n" +
							"(60) cpvar (59)\n" +
							"(71) target/testTemp/applications/lineage_trace/LineageTraceSelfReferenceDMLTest/out/X\n" +
							"(72) textcell\n" +
							"(73) write (60) (71) (72)\n";
			String expected_Y_lineage =
					"(0) target/testTemp/applications/lineage_trace/LineageTraceSelfReferenceDMLTest/in/X\n" +
							"(1) false\n" +
							"(2) createvar (0) (1)\n" +
							"(6) rblk (2)\n" +
							"(9) cpvar (6)\n" +
							"(21) 7\n" +
							"(22) + (9) (21)\n" +
							"(23) cpvar (22)\n" +
							"(38) * (23) (23)\n" +
							"(42) * (38) (23)\n" +
							"(43) cpvar (42)\n" +
							"(58) 7\n" +
							"(59) + (43) (58)\n" +
							"(60) cpvar (59)\n" +
							"(70) tsmm (60)\n" +
							"(74) target/testTemp/applications/lineage_trace/LineageTraceSelfReferenceDMLTest/out/Y\n" +
							"(75) textcell\n" +
							"(76) write (70) (74) (75)\n";
			
			LineageItem.resetIDSequence();
			runTest(true, EXCEPTION_NOT_EXPECTED, null, -1);
			
			String X_lineage = readDMLLineageFromHDFS("X");
			String Y_lineage = readDMLLineageFromHDFS("Y");
			
//			TestUtils.compareScalars(expected_X_lineage, X_lineage);
//			TestUtils.compareScalars(expected_Y_lineage, Y_lineage);
		} finally {
			OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = old_simplification;
			OptimizerUtils.ALLOW_SUM_PRODUCT_REWRITES = old_sum_product;
		}
	}
}
