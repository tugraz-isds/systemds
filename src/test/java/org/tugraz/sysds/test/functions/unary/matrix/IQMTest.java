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

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;


public class IQMTest extends AutomatedTestBase 
{
	private enum TEST_TYPE { 
		IQM ("IQM");
		String scriptName = null;
		TEST_TYPE(String name) {
			this.scriptName = name;
		}
	}
	
	private final static String TEST_DIR = "functions/unary/matrix/";
	private static final String TEST_CLASS_DIR = TEST_DIR + IQMTest.class.getSimpleName() + "/";

	private final static String[] datasets = {
		"2.2 3.2 3.7 4.4 5.3 5.7 6.1 6.4 7.2 7.8",  // IQM = 5.3100000000000005
		"2 3 4 1 2 3 4",							// IQM = 2.7142857142857144
		"1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1",	// IQM = 1
		"1 1 1 1 1 1 1",							// IQM = 1
		"1 3 5 7 9 11 13 15 17",					// IQM = 9
		"3 3 3 1 1 7 7 7 7 5 5 9 9 13 13 11 17 15 15 15",	// IQM =8
		"-1 -3 -5 0 7 9 -11 13 15 17",						// IQM = 4
		"-3 -3 -3 -1 -1 7 7 0 0 0 0 -5 -5 9 13 13 13 -11 -11 17 15"		// IQM = 1.2857142857142858
	};
	
	private final static double[] dataLengths = { 10, 7, 19, 7, 9, 20, 10, 21};
	private final static double[] expectedResults = {5.3100000000000005, 2.7142857142857144, 1, 1, 9, 8, 4, 1.2857142857142858};
		
	private final static String[] weightedDatasets = {
		"1 1 1 1 1 1 1",					// weighted IQM = 1
		"1 3 5 7 9 11 13 15 17",			// weighted IQM = 8
		"-1 -3 -5   0 7  9 -11 13 15 17",	// weighted IQM = 1.2857142857142858
		"-1 -3 -5 -11 0  7   9 13 15 17"	// weighted IQM = 0
	};
		
	private final static String[] weights = {
		"2 3 4 1  2 3 4",
		"2 3 2 4  2 1 2 3 1",
		"2 3 2 4  2 1 2 3 1 1",
		"2 3 2 2 18 2 1 3 2 1"
	};

	private final static double[] weightedDataLengths = { 7, 9, 10, 10 };
	private final static double[] weigthtedExpectedResults = {1, 8, 1.2857142857142858, 0};
	
	@Override
	public void setUp() {
		availableTestConfigurations.put(TEST_TYPE.IQM.scriptName, new TestConfiguration(TEST_CLASS_DIR, TEST_TYPE.IQM.scriptName, new String[] { "iqmFile", "iqmWtFile" }));
	}
	
	@Test
	public void testIQM1() {
		runTest(ExecMode.HYBRID, 1, false);
	}
	
	@Test
	public void testIQM2() {
		runTest(ExecMode.HYBRID, 2, false);
	}
	
	@Test
	public void testIQM3() {
		runTest(ExecMode.HYBRID, 3, false);
	}
	
	@Test
	public void testIQM4() {
		runTest(ExecMode.HYBRID, 4, false);
	}
	
	@Test
	public void testIQM5() {
		runTest(ExecMode.HYBRID, 5, false);
	}
	
	@Test
	public void testIQM6() {
		runTest(ExecMode.HYBRID, 6, false);
	}
	
	@Test
	public void testIQM7() {
		runTest(ExecMode.HYBRID, 7, false);
	}
	
	@Test
	public void testIQM8() {
		runTest(ExecMode.HYBRID, 8, false);
	}
	
	@Test
	public void testIQM1_SP() {
		runTest(ExecMode.SPARK, 1, false);
	}
	
	@Test
	public void testIQM2_SP() {
		runTest(ExecMode.SPARK, 2, false);
	}
	
	@Test
	public void testIQM3_SP() {
		runTest(ExecMode.SPARK, 3, false);
	}
	
	@Test
	public void testIQM4_SP() {
		runTest(ExecMode.SPARK, 4, false);
	}
	
	@Test
	public void testIQM5_SP() {
		runTest(ExecMode.SPARK, 5, false);
	}
	
	@Test
	public void testIQM6_SP() {
		runTest(ExecMode.SPARK, 6, false);
	}
	
	@Test
	public void testIQM7_SP() {
		runTest(ExecMode.SPARK, 7, false);
	}
	
	@Test
	public void testIQM8_SP() {
		runTest(ExecMode.SPARK, 8, false);
	}
	
	@Test
	public void testIQM1wt() {
		runTest(ExecMode.HYBRID, 1, true);
	}
	
	@Test
	public void testIQM2wt() {
		runTest(ExecMode.HYBRID, 2, true);
	}
	
	@Test
	public void testIQM3wt() {
		runTest(ExecMode.HYBRID, 3, true);
	}
	
	@Test
	public void testIQM4wt() {
		runTest(ExecMode.HYBRID, 4, true);
	}
	
	@Test
	public void testIQM1wt_SP() {
		runTest(ExecMode.SPARK, 1, true);
	}
	
	@Test
	public void testIQM2wt_SP() {
		runTest(ExecMode.SPARK, 2, true);
	}
	
	@Test
	public void testIQM3wt_SP() {
		runTest(ExecMode.SPARK, 3, true);
	}
	
	@Test
	public void testIQM4wt_SP() {
		runTest(ExecMode.SPARK, 4, true);
	}
	
	
	private void runTest(ExecMode rt, int datasetIndex, boolean isWeighted ) {
		ExecMode rtOld = rtplatform;
		rtplatform = rt;
		
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		if( rtplatform == ExecMode.SPARK )
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;
		
		try
		{
			TEST_TYPE test = TEST_TYPE.IQM;
			TestConfiguration config = getTestConfiguration(test.scriptName);
			
			int rows;
			double expectedIQM;
			String dataString = null, weightsString = null;
	
			if(isWeighted) {
				rows = (int) weightedDataLengths[datasetIndex-1];
				expectedIQM = weigthtedExpectedResults[datasetIndex-1];
				dataString = weightedDatasets[datasetIndex-1];
				weightsString = weights[datasetIndex-1];
			}
			else {
				rows = (int) dataLengths[datasetIndex-1];
				expectedIQM = expectedResults[datasetIndex-1];
				dataString = datasets[datasetIndex-1];
				// construct weights string
				weightsString = "1";
				int i=1;
				while(i<rows) {
					weightsString += " 1";
					i++;
				}
			}
			
			config.addVariable("rows", rows);
			config.addVariable("cols", 1);
			loadTestConfiguration(config);
			
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + test.scriptName + ".dml";
			String outFile = output("iqmFile");
			String wtOutFile = output("iqmWtFile");
			programArgs = new String[]{"-args", dataString, weightsString, Integer.toString(rows),
				outFile, wtOutFile };
			
			runTest(true, false, null, -1);
			
			double IQM = TestUtils.readDMLScalar(outFile);
			double wtIQM = TestUtils.readDMLScalar(wtOutFile);
			
			if(isWeighted) {
				assertTrue("Incorrect weighted inter quartile mean "+wtIQM+" vs "+expectedIQM, wtIQM == expectedIQM);
			}
			else {
				assertTrue("Incorrect inter quartile mean "+wtIQM+" vs "+IQM, wtIQM == IQM);
				assertTrue("Incorrect inter quartile mean "+wtIQM+" vs "+expectedIQM, wtIQM == expectedIQM);
			}
		}
		finally {
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
			rtplatform = rtOld;
		}
	}
}
