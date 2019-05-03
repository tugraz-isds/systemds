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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.runtime.lineage.LineageItem;
import org.tugraz.sysds.runtime.lineage.LineageParser;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;
import org.tugraz.sysds.utils.Explain;

public class LineageTraceEqualsTest extends AutomatedTestBase {
	
	protected static final String TEST_DIR = "functions/lineage/";
	protected static final String TEST_NAME1 = "LineageTraceEquals1";
	protected String TEST_CLASS_DIR = TEST_DIR + LineageTraceEqualsTest.class.getSimpleName() + "/";
	
	protected static final int numRecords = 10;
	protected static final int numFeatures = 5;
	
	
	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1));
	}
	
	@Test
	public void testLineageTrace1() {
		testLineageTrace(TEST_NAME1);
	}
	
	public void testLineageTrace(String testname) {
		boolean old_simplification = OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION;
		boolean old_sum_product = OptimizerUtils.ALLOW_SUM_PRODUCT_REWRITES;
		
		try {
			System.out.println("------------ BEGIN " + testname + "------------");
			
			OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = false;
			OptimizerUtils.ALLOW_SUM_PRODUCT_REWRITES = false;
			
			int rows = numRecords;
			int cols = numFeatures;
			
			getAndLoadTestConfiguration(testname);
			
			List<String> proArgs = new ArrayList<String>();
			
			proArgs.add("-stats");
			proArgs.add("-lineage");
			proArgs.add("-explain");
			proArgs.add("-args");
			proArgs.add(input("M"));
			proArgs.add(output("X"));
			proArgs.add(output("Z"));
			programArgs = proArgs.toArray(new String[proArgs.size()]);
			
			fullDMLScriptName = getScript();
			
			double[][] m = getRandomMatrix(rows, cols, 0, 1, 0.8, -1);
			writeInputMatrixWithMTD("M", m, true);
			
			LineageItem.resetIDSequence();
			runTest(true, EXCEPTION_NOT_EXPECTED, null, -1);
			
			String X_lineage = readDMLLineageFromHDFS("X");
			String Z_lineage = readDMLLineageFromHDFS("Z");
			
			LineageItem X_li = LineageParser.parseLineageTrace(X_lineage);
			LineageItem Z_li = LineageParser.parseLineageTrace(Z_lineage );
			
			boolean equal = X_li.equals(Z_li);
			System.out.print("Test");
			//			TestUtils.compareScalars(X_lineage, Explain.explain(X_li));
//			TestUtils.compareScalars(Y_lineage, Explain.explain(Y_li));
		} finally {
			OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = old_simplification;
			OptimizerUtils.ALLOW_SUM_PRODUCT_REWRITES = old_sum_product;
		}
	}
}
