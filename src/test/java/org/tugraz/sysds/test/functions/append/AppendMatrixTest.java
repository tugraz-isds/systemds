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

package org.tugraz.sysds.test.functions.append;

import java.util.HashMap;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.hops.BinaryOp;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.hops.BinaryOp.AppendMethod;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;
import org.tugraz.sysds.utils.Statistics;

public class AppendMatrixTest extends AutomatedTestBase
{
	
	private final static String TEST_NAME = "AppendMatrixTest";
	private final static String TEST_DIR = "functions/append/";
	private final static String TEST_CLASS_DIR = TEST_DIR + AppendMatrixTest.class.getSimpleName() + "/";

	private final static double epsilon=0.0000000001;
	private final static int min=1;
	private final static int max=100;
	
	private final static int rows = 1692;
	//usecase a: inblock single
	private final static int cols1a = 375;
	private final static int cols2a = 92;
	//usecase b: inblock multiple
	private final static int cols1b = 1059;
	private final static int cols2b = 1010;
	//usecase c: outblock blocksize 
	private final static int cols1c = 2*OptimizerUtils.DEFAULT_BLOCKSIZE;
	private final static int cols2c = 1010;
	//usecase d: outblock blocksize 
	private final static int cols1d = 1460;
	private final static int cols2d = 1920;
	private final static int cols3d = 990;
	
		
	private final static double sparsity1 = 0.5;
	private final static double sparsity2 = 0.01;
	
	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, 
				new String[] {"C"}));
	}

	@Test
	public void testAppendInBlock1DenseCP() {
		commonAppendTest(ExecMode.SINGLE_NODE, rows, cols1a, cols2a, false, null);
	}
	
	@Test
	public void testAppendInBlock1SparseCP() {
		commonAppendTest(ExecMode.SINGLE_NODE, rows, cols1a, cols2a, true, null);
	}
	
	// -----------------------------------------------------------------
	
	@Test
	public void testAppendInBlock1DenseSP() {
		commonAppendTest(ExecMode.SPARK, rows, cols1a, cols2a, false, AppendMethod.MR_RAPPEND);
	}   
	
	@Test
	public void testAppendInBlock1SparseSP() {
		commonAppendTest(ExecMode.SPARK, rows, cols1a, cols2a, true, AppendMethod.MR_RAPPEND);
	}   
	
	//NOTE: mappend only applied for m2_cols<=blocksize
	@Test
	public void testMapAppendInBlock2DenseSP() {
		commonAppendTest(ExecMode.SPARK, rows, cols1b, cols2a, false, AppendMethod.MR_MAPPEND);
	}
	
	@Test
	public void testMapAppendInBlock2SparseSP() {
		commonAppendTest(ExecMode.SPARK, rows, cols1b, cols2a, true, AppendMethod.MR_MAPPEND);
	}
	
	@Test
	public void testMapAppendOutBlock2DenseSP() {
		commonAppendTest(ExecMode.SPARK, rows, cols1d, cols3d, false, AppendMethod.MR_MAPPEND);
	}
	
	@Test
	public void testMapAppendOutBlock2SparseSP() {
		commonAppendTest(ExecMode.SPARK, rows, cols1d, cols3d, true, AppendMethod.MR_MAPPEND);
	}
	
	@Test
	public void testAppendOutBlock1DenseSP() {
		commonAppendTest(ExecMode.SPARK, rows, cols1c, cols2c, false, AppendMethod.SP_GAlignedAppend);
	}
	
	@Test
	public void testAppendOutBlock1SparseSP() {
		commonAppendTest(ExecMode.SPARK, rows, cols1c, cols2c, true, AppendMethod.SP_GAlignedAppend);
	}
	
	@Test
	public void testAppendInBlock2DenseSP() {
		commonAppendTest(ExecMode.SPARK, rows, cols1b, cols2b, false, AppendMethod.MR_GAPPEND);
	}
	
	@Test
	public void testAppendInBlock2SparseSP() {
		commonAppendTest(ExecMode.SPARK, rows, cols1b, cols2b, true, AppendMethod.MR_GAPPEND);
	}
	
	@Test
	public void testAppendOutBlock2DenseSP() {
		commonAppendTest(ExecMode.SPARK, rows, cols1d, cols2d, false, AppendMethod.MR_GAPPEND);
	}
	
	@Test
	public void testAppendOutBlock2SparseSP() {
		commonAppendTest(ExecMode.SPARK, rows, cols1d, cols2d, true, AppendMethod.MR_GAPPEND);
	}
	
	// -----------------------------------------------------------------
	
	//NOTE: different dimension use cases only relvant for MR
	/*
	@Test
	public void testAppendInBlock2CP() {
		commonAppendTest(ExecMode.SINGLE_NODE, rows, cols1b, cols2b);
	}
	
	@Test
	public void testAppendOutBlock1CP() {
		commonAppendTest(ExecMode.SINGLE_NODE, rows, cols1c, cols2c);
	}	

	@Test
	public void testAppendOutBlock2CP() {
		commonAppendTest(ExecMode.SINGLE_NODE, rows, cols1d, cols2d);
	}*/
	public void commonAppendTest(ExecMode platform, int rows, int cols1, int cols2, boolean sparse, AppendMethod forcedAppendMethod)
	{
		TestConfiguration config = getAndLoadTestConfiguration(TEST_NAME);
	    
		ExecMode prevPlfm=rtplatform;
		
		double sparsity = (sparse) ? sparsity2 : sparsity1; 
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		
		try
		{
			if(forcedAppendMethod != null) {
				BinaryOp.FORCED_APPEND_METHOD = forcedAppendMethod;
			}
		    rtplatform = platform;
		    if( rtplatform == ExecMode.SPARK )
				DMLScript.USE_LOCAL_SPARK_CONFIG = true;
	
	        config.addVariable("rows", rows);
	        config.addVariable("cols", cols1);
	          
			/* This is for running the junit test the new way, i.e., construct the arguments directly */
			String RI_HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = RI_HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-args",  input("A"), 
					                             Long.toString(rows), 
					                             Long.toString(cols1),
								                 input("B"),
								                 Long.toString(cols2),
		                                         output("C") };
			fullRScriptName = RI_HOME + TEST_NAME + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + 
			       inputDir() + " " + expectedDir();
	
			Random rand=new Random(System.currentTimeMillis());
			double[][] A = getRandomMatrix(rows, cols1, min, max, sparsity, System.currentTimeMillis());
	        writeInputMatrix("A", A, true);
	        sparsity=rand.nextDouble();
	        double[][] B= getRandomMatrix(rows, cols2, min, max, sparsity, System.currentTimeMillis());
	        writeInputMatrix("B", B, true);
	        
	        boolean exceptionExpected = false;
	        int expectedCompiledMRJobs = 1;
			int expectedExecutedMRJobs = 0;
			runTest(true, exceptionExpected, null, expectedCompiledMRJobs);
			runRScript(true);
			Assert.assertEquals("Wrong number of executed MR jobs.",
				expectedExecutedMRJobs, Statistics.getNoOfExecutedSPInst());
	
			for(String file: config.getOutputFiles())
			{
				HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS(file);
				HashMap<CellIndex, Double> rfile = readRMatrixFromFS(file);
				TestUtils.compareMatrices(dmlfile, rfile, epsilon, file+"-DML", file+"-R");
			}
		}
		finally
		{
			//reset execution platform
			rtplatform = prevPlfm;
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
			BinaryOp.FORCED_APPEND_METHOD = null;
		}
	}
   
}
