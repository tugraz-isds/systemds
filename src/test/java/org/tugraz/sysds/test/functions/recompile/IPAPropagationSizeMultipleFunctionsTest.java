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

package org.tugraz.sysds.test.functions.recompile;

import java.util.HashMap;

import org.junit.Test;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class IPAPropagationSizeMultipleFunctionsTest extends AutomatedTestBase 
{
	private final static String TEST_NAME1 = "multiple_function_calls1";
	private final static String TEST_NAME2 = "multiple_function_calls2";
	private final static String TEST_NAME3 = "multiple_function_calls3";
	private final static String TEST_NAME4 = "multiple_function_calls4";
	private final static String TEST_NAME5 = "multiple_function_calls5";
	
	private final static String TEST_DIR = "functions/recompile/";
	private final static String TEST_CLASS_DIR = TEST_DIR + 
		IPAPropagationSizeMultipleFunctionsTest.class.getSimpleName() + "/";
	
	private final static int rows = 10;
	private final static int cols = 15;    
	private final static double sparsity = 0.7;
	
	@Override
	public void setUp() 
	{
		TestUtils.clearAssertionInformation();
		addTestConfiguration( TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] { "R" }) );
		addTestConfiguration( TEST_NAME2, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2, new String[] { "R" }) );
		addTestConfiguration( TEST_NAME3, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME3, new String[] { "R" }) );
		addTestConfiguration( TEST_NAME4, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME4, new String[] { "R" }) );
		addTestConfiguration( TEST_NAME5, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME5, new String[] { "R" }) );
	}
	
	
	@Test
	public void testFunctionSizePropagationSameInput() {
		runIPASizePropagationMultipleFunctionsTest(TEST_NAME1, false);
	}
	
	@Test
	public void testFunctionSizePropagationEqualDimsUnknownNnzRight() {
		runIPASizePropagationMultipleFunctionsTest(TEST_NAME2, false);
	}
	
	@Test
	public void testFunctionSizePropagationEqualDimsUnknownNnzLeft() {
		runIPASizePropagationMultipleFunctionsTest(TEST_NAME3, false);
	}
	
	@Test
	public void testFunctionSizePropagationEqualDimsUnknownNnz() {
		runIPASizePropagationMultipleFunctionsTest(TEST_NAME4, false);
	}
	
	@Test
	public void testFunctionSizePropagationDifferentDimsUnary() {
		runIPASizePropagationMultipleFunctionsTest(TEST_NAME5, false);
	}
	
	@Test
	public void testFunctionSizePropagationSameInputIPA() {
		runIPASizePropagationMultipleFunctionsTest(TEST_NAME1, true);
	}
	
	@Test
	public void testFunctionSizePropagationEqualDimsUnknownNnzRightIPA() {
		runIPASizePropagationMultipleFunctionsTest(TEST_NAME2, true);
	}
	
	@Test
	public void testFunctionSizePropagationEqualDimsUnknownNnzLeftIPA() {
		runIPASizePropagationMultipleFunctionsTest(TEST_NAME3, true);
	}
	
	@Test
	public void testFunctionSizePropagationEqualDimsUnknownNnzIPA() {
		runIPASizePropagationMultipleFunctionsTest(TEST_NAME4, true);
	}
	
	@Test
	public void testFunctionSizePropagationDifferentDimsIPAUnary() {
		runIPASizePropagationMultipleFunctionsTest(TEST_NAME5, true);
	}
	
	private void runIPASizePropagationMultipleFunctionsTest( String TEST_NAME, boolean IPA )
	{	
		boolean oldFlagIPA = OptimizerUtils.ALLOW_INTER_PROCEDURAL_ANALYSIS;
		
		try
		{
			TestConfiguration config = getTestConfiguration(TEST_NAME);
			loadTestConfiguration(config);
			
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-explain","-args", input("V"), output("R") };
			
			fullRScriptName = HOME + TEST_NAME + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " + expectedDir();
			
			OptimizerUtils.ALLOW_INTER_PROCEDURAL_ANALYSIS = IPA;
			
			//generate input data
			double[][] V = getRandomMatrix(rows, cols, 0, 1, sparsity, 7);
			writeInputMatrixWithMTD("V", V, true);
	
			//run tests
			runTest(true, false, null, -1); 
			runRScript(true); 
			
			//compare matrices 
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("R");
			HashMap<CellIndex, Double> rfile  = readRMatrixFromFS("R");
			TestUtils.compareMatrices(dmlfile, rfile, 0, "Stat-DML", "Stat-R");
			
			//check expected number of compiled and executed MR jobs
			int expectedNumCompiled = (IPA) ? ((TEST_NAME.equals(TEST_NAME5))?2:1) : 
				(TEST_NAME.equals(TEST_NAME5)?5:4); //reblock, 2xGMR foo, GMR 
			int expectedNumExecuted = 0;
			
			checkNumCompiledSparkInst(expectedNumCompiled); 
			checkNumExecutedSparkInst(expectedNumExecuted); 
		}
		finally {
			OptimizerUtils.ALLOW_INTER_PROCEDURAL_ANALYSIS = oldFlagIPA;
		}
	}
	
}
