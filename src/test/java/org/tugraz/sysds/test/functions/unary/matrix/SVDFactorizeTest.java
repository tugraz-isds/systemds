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

public class SVDFactorizeTest extends AutomatedTestBase 
{
	private final static String TEST_NAME1 = "svd";
	private final static String TEST_DIR = "functions/unary/matrix/";
	private static final String TEST_CLASS_DIR = TEST_DIR + SVDFactorizeTest.class.getSimpleName() + "/";

	private final static int rows1 = 500;
	private final static int rows2 = 1500;
	private final static int cols1 = 400;
	private final static int cols2 = 1200;
	private final static double sparsity = 0.9;
	
	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME1, 
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] { "D" }));
	}
	
	@Test
	public void testSVDFactorizeDenseCP() {
		runTestSVDFactorize( rows1, cols1, ExecMode.SINGLE_NODE );
	}
	
	@Test
	public void testSVDFactorizeDenseSP() {
		runTestSVDFactorize( rows1, cols1, ExecMode.SPARK );
	}
	
	@Test
	public void testSVDFactorizeDenseHybrid() {
		runTestSVDFactorize( rows1, cols1, ExecMode.HYBRID );
	}
	
	@Test
	public void testLargeSVDFactorizeDenseCP() {
		runTestSVDFactorize( rows2, cols2, ExecMode.SINGLE_NODE );
	}
	
	@Test
	public void testLargeSVDFactorizeDenseSP() {
		runTestSVDFactorize( rows2, cols2, ExecMode.SPARK );
	}
	
	@Test
	public void testLargeSVDFactorizeDenseHybrid() {
		runTestSVDFactorize( rows2, cols2, ExecMode.HYBRID );
	}
	
	private void runTestSVDFactorize( int rows, int cols, ExecMode rt)
	{
		ExecMode rtold = rtplatform;
		rtplatform = rt;
		
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		if( rtplatform == ExecMode.SPARK )
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;
		
		try
		{
			getAndLoadTestConfiguration(TEST_NAME1);
			
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME1 + ".dml";
			programArgs = new String[]{"-args", input("A"), output("D") };
			
			double[][] A = getRandomMatrix(rows, cols, 0, 1, sparsity, 10);
			MatrixCharacteristics mc = new MatrixCharacteristics(rows, cols, -1, -1, -1);
			writeInputMatrixWithMTD("A", A, false, mc);
			
			// Expected matrix = 1x1 zero matrix 
			double[][] D  = new double[1][1];
			D[0][0] = 0.0;
			writeExpectedMatrix("D", D);
			
			runTest(true, false, null, -1);
			compareResults(1e-8);
		}
		finally {
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
			rtplatform = rtold;
			
		}
	}
}
