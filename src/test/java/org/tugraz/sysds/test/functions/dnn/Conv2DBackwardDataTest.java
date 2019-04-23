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
package org.tugraz.sysds.test.functions.dnn;

import java.util.HashMap;

import org.junit.Test;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.runtime.util.DnnUtils;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class Conv2DBackwardDataTest extends AutomatedTestBase
{
	
	private final static String TEST_NAME = "Conv2DBackwardDataTest";
	private final static String TEST_DIR = "functions/tensor/";
	private final static String TEST_CLASS_DIR = TEST_DIR + Conv2DBackwardDataTest.class.getSimpleName() + "/";
	private final static double epsilon=0.0000000001;
	
	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, 
				new String[] {"B"}));
	}
	
	@Test
	public void testConv2DBwdDataDense1() 
	{
		int numImg = 2; int imgSize = 10; int numChannels = 3; int numFilters = 2; int filterSize = 2; int stride = 1; int pad = 0;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	@Test
	public void testConv2DDense2() 
	{
		int numImg = 5; int imgSize = 3; int numChannels = 2; int numFilters = 3; int filterSize = 3; int stride = 1; int pad = 1;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	@Test
	public void testConv2DDense3() 
	{
		int numImg = 5; int imgSize = 3; int numChannels = 2; int numFilters = 3; int filterSize = 3; int stride = 2; int pad = 1;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	@Test
	public void testConv2DBwdDataDense4() 
	{
		int numImg = 5; int imgSize = 10; int numChannels = 2; int numFilters = 3; int filterSize = 2; int stride = 2; int pad = 1;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	@Test
	public void testConv2DBwdDataSparse1() 
	{
		int numImg = 2; int imgSize = 10; int numChannels = 3; int numFilters = 2; int filterSize = 2; int stride = 1; int pad = 0;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, true, false);
	}
	
	@Test
	public void testConv2DBwdDataSparse2() 
	{
		int numImg = 5; int imgSize = 3; int numChannels = 2; int numFilters = 3; int filterSize = 3; int stride = 1; int pad = 1;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	@Test
	public void testConv2DBwdDataSparse3() 
	{
		int numImg = 5; int imgSize = 3; int numChannels = 2; int numFilters = 3; int filterSize = 3; int stride = 2; int pad = 1;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, true);
	}
	
	@Test
	public void testConv2DBwdDataSparse4() 
	{
		int numImg = 5; int imgSize = 10; int numChannels = 2; int numFilters = 3; int filterSize = 2; int stride = 2; int pad = 1;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, true, true);
	}
	
	@Test
	public void testConv2DBwdDataSparse5() 
	{
		int numImg = 5; int imgSize = 3; int numChannels = 2; int numFilters = 3; int filterSize = 3; int stride = 1; int pad = 1;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, true, false);
	}
	
	@Test
	public void testConv2DBwdDataSparse6() 
	{
		int numImg = 5; int imgSize = 3; int numChannels = 2; int numFilters = 3; int filterSize = 3; int stride = 2; int pad = 1;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, true, true);
	}
	
	@Test
	public void testConv2DBwdDataSparse7() 
	{
		int numImg = 5; int imgSize = 10; int numChannels = 2; int numFilters = 3; int filterSize = 2; int stride = 2; int pad = 1;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, true, true);
	}
	
	
	
	
	/**
	 * 
	 * @param et
	 * @param sparse
	 */
	public void runConv2DTest( ExecType et, int imgSize, int numImg, int numChannels, int numFilters, 
			int filterSize, int stride, int pad, boolean sparse1, boolean sparse2) 
	{
		ExecMode oldRTP = rtplatform;
			
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		
		try
		{
	    TestConfiguration config = getTestConfiguration(TEST_NAME);
	    if(et == ExecType.SPARK) {
	    	rtplatform = ExecMode.SPARK;
	    }
	    else {
	    	rtplatform = ExecMode.SINGLE_NODE;
	    }
			if( rtplatform == ExecMode.SPARK )
				DMLScript.USE_LOCAL_SPARK_CONFIG = true;
			
			loadTestConfiguration(config);
	        
			/* This is for running the junit test the new way, i.e., construct the arguments directly */
			String RI_HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = RI_HOME + TEST_NAME + ".dml";
			
			String sparseVal1 = (""+sparse1).toUpperCase();
			String sparseVal2 = (""+sparse2).toUpperCase();
			
			long P = DnnUtils.getP(imgSize, filterSize, stride, pad);
			programArgs = new String[]{"-explain", "-args",  "" + imgSize, "" + numImg, 
					"" + numChannels, "" + numFilters, 
					"" + filterSize, "" + stride, "" + pad,
					"" + P, "" + P, 
					output("B"), sparseVal1, sparseVal2};
			        
			boolean exceptionExpected = false;
			int expectedNumberOfJobs = -1;
			runTest(true, exceptionExpected, null, expectedNumberOfJobs);
			
			fullRScriptName = RI_HOME + TEST_NAME + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + imgSize + " " + numImg + 
					" " + numChannels + " " + numFilters + 
					" " + filterSize + " " + stride + " " + pad + " " + P + " " + P + " " + expectedDir() +
					" " + sparseVal1 + " " + sparseVal2;
			// Run comparison R script
			runRScript(true);
			HashMap<CellIndex, Double> bHM = readRMatrixFromFS("B");
			
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("B");
			TestUtils.compareMatrices(dmlfile, bHM, epsilon, "B-DML", "NumPy");
			
		}
		finally
		{
			rtplatform = oldRTP;
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
		}
		
	}
	
}
