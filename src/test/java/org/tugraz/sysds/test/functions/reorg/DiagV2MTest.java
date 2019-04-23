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

package org.tugraz.sysds.test.functions.reorg;

import java.util.HashMap;
import java.util.Random;

import org.junit.Test;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class DiagV2MTest extends AutomatedTestBase
{
	private final static String TEST_DIR = "functions/reorg/";
	private static final String TEST_CLASS_DIR = TEST_DIR + DiagV2MTest.class.getSimpleName() + "/";

	private final static double epsilon=0.0000000001;
	private final static int rows = 1059;
	private final static int min=0;
	private final static int max=100;
	
	@Override
	public void setUp() {
		addTestConfiguration("DiagV2MTest", 
			new TestConfiguration(TEST_CLASS_DIR, "DiagV2MTest", new String[] {"C"}));
	}
	
	public void commonReorgTest(ExecMode platform)
	{
		TestConfiguration config = getTestConfiguration("DiagV2MTest");
	    
		ExecMode prevPlfm=rtplatform;
		
	    rtplatform = platform;
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		if( rtplatform == ExecMode.SPARK )
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;

		try {
	        config.addVariable("rows", rows);
			loadTestConfiguration(config);
	          
			/* This is for running the junit test the new way, i.e., construct the arguments directly */
			String RI_HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = RI_HOME + "DiagV2MTest" + ".dml";
			programArgs = new String[]{"-explain", "-args",  input("A"), Long.toString(rows), output("C") };
			
			fullRScriptName = RI_HOME + "DiagV2MTest" + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " + expectedDir();
	
			Random rand=new Random(System.currentTimeMillis());
			double sparsity=0.599200924665577;//rand.nextDouble();
			double[][] A = getRandomMatrix(rows, 1, min, max, sparsity, 1397289950533L); // System.currentTimeMillis()
	        writeInputMatrix("A", A, true);
	        sparsity=rand.nextDouble();   
			
	        boolean exceptionExpected = false;
			int expectedNumberOfJobs = -1;
			runTest(true, exceptionExpected, null, expectedNumberOfJobs);
			
			runRScript(true);
		
			for(String file: config.getOutputFiles())
			{
				HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS(file);
				HashMap<CellIndex, Double> rfile = readRMatrixFromFS(file);
			//	System.out.println(file+"-DML: "+dmlfile);
			//	System.out.println(file+"-R: "+rfile);
				TestUtils.compareMatrices(dmlfile, rfile, epsilon, file+"-DML", file+"-R");
			}
		}
		finally {
			rtplatform = prevPlfm;
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
		}
	}
	
	@Test
	public void testDiagV2MCP() {
		commonReorgTest(ExecMode.SINGLE_NODE);
	}
	
	@Test
	public void testDiagV2MSP() {
		commonReorgTest(ExecMode.SPARK);
	}
}

