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

import org.junit.Assert;
import org.junit.Test;
import org.tugraz.sysds.conf.CompilerConfig;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;
import org.tugraz.sysds.utils.Statistics;

public class FunctionRecompileTest extends AutomatedTestBase 
{
	private final static String TEST_NAME1 = "funct_recompile";
	private final static String TEST_DIR = "functions/recompile/";
	private final static String TEST_CLASS_DIR = TEST_DIR + FunctionRecompileTest.class.getSimpleName() + "/";
	private final static double eps = 1e-10;
	
	private final static int rows = 20;
	private final static int cols = 10;
	private final static double sparsity = 1.0;
	
	@Override
	public void setUp()  {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME1, 
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] { "Rout" }) );
	}

	@Test
	public void testFunctionWithoutRecompileWithoutIPA() {
		runFunctionTest(false, false);
	}
	
	@Test
	public void testFunctionWithoutRecompileWithIPA() {
		runFunctionTest(false, true);
	}

	@Test
	public void testFunctionWithRecompileWithoutIPA() {
		runFunctionTest(true, false);
	}
	
	@Test
	public void testFunctionWithRecompileWithIPA() {
		runFunctionTest(true, true);
	}

	private void runFunctionTest( boolean recompile, boolean IPA )
	{	
		boolean oldFlagRecompile = CompilerConfig.FLAG_DYN_RECOMPILE;
		boolean oldFlagIPA = OptimizerUtils.ALLOW_INTER_PROCEDURAL_ANALYSIS;
		
		try
		{
			TestConfiguration config = getTestConfiguration(TEST_NAME1);
			config.addVariable("rows", rows);
			config.addVariable("cols", cols);
			loadTestConfiguration(config);
			
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME1 + ".dml";
			programArgs = new String[]{"-explain", "-args", input("V"), 
				Integer.toString(rows), Integer.toString(cols), output("R") };
			
			fullRScriptName = HOME + TEST_NAME1 + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " + expectedDir();
	
			long seed = System.nanoTime();
			double[][] V = getRandomMatrix(rows, cols, 0, 1, sparsity, seed);
			writeInputMatrix("V", V, true);
	
			CompilerConfig.FLAG_DYN_RECOMPILE = recompile;
			OptimizerUtils.ALLOW_INTER_PROCEDURAL_ANALYSIS = IPA;
			
			boolean exceptionExpected = false;
			runTest(true, exceptionExpected, null, -1); 
			runRScript(true);
			
			//note: change from previous version due to fix in op selection (unknown size XtX and mapmult)
			//CHECK compiled MR jobs
			int expectNumCompiled = -1;
			if( IPA ) expectNumCompiled = 1; //reblock
			else      expectNumCompiled = 5; //reblock, GMR,GMR,GMR,GMR (last two should piggybacked)
			Assert.assertEquals("Unexpected number of compiled MR jobs.", 
				expectNumCompiled, Statistics.getNoOfCompiledSPInst());
		
			//CHECK executed MR jobs
			int expectNumExecuted = -1;
			if( recompile ) expectNumExecuted = 0;
			else if( IPA )  expectNumExecuted = 1; //reblock
			else            expectNumExecuted = 41; //reblock, 10*(GMR,GMR,GMR, GMR) (last two should piggybacked)
			Assert.assertEquals("Unexpected number of executed MR jobs.",
				expectNumExecuted, Statistics.getNoOfExecutedSPInst());
			
			//compare matrices
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("R");
			HashMap<CellIndex, Double> rfile  = readRMatrixFromFS("Rout");
			TestUtils.compareMatrices(dmlfile, rfile, eps, "DML", "R");
		}
		finally {
			CompilerConfig.FLAG_DYN_RECOMPILE = oldFlagRecompile;
			OptimizerUtils.ALLOW_INTER_PROCEDURAL_ANALYSIS = oldFlagIPA;
		}
	}
}
