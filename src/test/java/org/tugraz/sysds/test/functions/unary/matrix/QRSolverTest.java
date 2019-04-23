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

import org.junit.Test;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.runtime.meta.MatrixCharacteristics;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;

public class QRSolverTest extends AutomatedTestBase 
{
	private final static String TEST_NAME = "QRsolve";
	private final static String TEST_DIR = "functions/unary/matrix/";
	private static final String TEST_CLASS_DIR = TEST_DIR + QRSolverTest.class.getSimpleName() + "/";

	private final static int rows = 1500;
	private final static int cols = 50;
	private final static double sparsity = 0.7;
	
	@Override
	public void setUp() 
	{
		addTestConfiguration(TEST_NAME, 
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] { "x" }) ); 

		// *** GENERATE data ONCE, and use it FOR ALL tests involving w/ different platforms
		// Therefore, data generation is done in setUp() method.
		
		TestConfiguration config = getAndLoadTestConfiguration(TEST_NAME);
		
		String HOME = SCRIPT_DIR + TEST_DIR;
		fullDMLScriptName = HOME + TEST_NAME + ".dml";
		programArgs = new String[]{"-args", input("A"), input("y"), 
			output(config.getOutputFiles()[0]) };

		fullRScriptName = HOME + TEST_NAME + ".R";
		rCmd = "Rscript" + " " + fullRScriptName + " " + input("A.mtx") + " " + 
			input("y.mtx") + " " + expected(config.getOutputFiles()[0]);
		
		double[][] A = getRandomMatrix(rows, cols, 0, 1, sparsity, 10);
		MatrixCharacteristics mc = new MatrixCharacteristics(rows, cols, -1, -1, -1);
		writeInputMatrixWithMTD("A", A, true, mc);
		
		double[][] y = getRandomMatrix(rows, 1, 0, 1, 1.0, 11);
		mc.set(rows, 1, -1, -1);
		writeInputMatrixWithMTD("y", y, true, mc);
	}
	
	@Test
	public void testQRSolveCP() {
		runTestQRSolve( ExecMode.SINGLE_NODE );
	}
	
	@Test
	public void testQRSolveSP() {
		runTestQRSolve( ExecMode.SPARK );
	}
	
	@Test
	public void testQRSolveHybrid() {
		runTestQRSolve( ExecMode.HYBRID );
	}
	
	private void runTestQRSolve( ExecMode rt)
	{
		ExecMode rtold = rtplatform;
		rtplatform = rt;
		
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		if( rtplatform == ExecMode.SPARK )
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;
		
		try {
			boolean exceptionExpected = false;
			runTest(true, exceptionExpected, null, -1);
			runRScript(true);
		
			compareResultsWithR(1e-5);			
		}
		finally {
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
			rtplatform = rtold;
		}
	}	
}