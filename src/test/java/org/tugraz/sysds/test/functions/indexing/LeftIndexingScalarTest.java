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

package org.tugraz.sysds.test.functions.indexing;

import java.util.HashMap;

import org.junit.Test;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.runtime.meta.MatrixCharacteristics;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class LeftIndexingScalarTest extends AutomatedTestBase
{

	private final static String TEST_DIR = "functions/indexing/";
	private final static String TEST_NAME = "LeftIndexingScalarTest";
	private final static String TEST_CLASS_DIR = TEST_DIR + LeftIndexingScalarTest.class.getSimpleName() + "/";
	
	private final static double epsilon=0.0000000001;
	private final static int rows = 1279;
	private final static int cols = 1050;

	private final static double sparsity = 0.7;
	private final static int min = 0;
	private final static int max = 100;
	
	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] {"A"}));
	}

	@Test
	public void testLeftIndexingScalarCP() 
	{
		runLeftIndexingTest(ExecType.CP);
	}
	
	@Test
	public void testLeftIndexingScalarSP() 
	{
		runLeftIndexingTest(ExecType.SPARK);
	}
	
	private void runLeftIndexingTest( ExecType instType ) 
	{		
		//rtplatform for MR
		ExecMode platformOld = rtplatform;
		if(instType == ExecType.SPARK) {
	    	rtplatform = ExecMode.SPARK;
	    }
	    else {
			rtplatform = ExecMode.HYBRID;
	    }
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		if( rtplatform == ExecMode.SPARK )
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;
		
	
		try
		{
			TestConfiguration config = getTestConfiguration(TEST_NAME);
			loadTestConfiguration(config);
			
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-explain" , "-args",  input("A"), 
				Long.toString(rows), Long.toString(cols), output("A")};
			
			fullRScriptName = HOME + TEST_NAME + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " + expectedDir();
			
	        double[][] A = getRandomMatrix(rows, cols, min, max, sparsity, System.currentTimeMillis());
	        writeInputMatrix("A", A, true);
	       
	        runTest(true, false, null, -1);		
			runRScript(true);
			
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("A");
			HashMap<CellIndex, Double> rfile = readRMatrixFromFS("A");
			TestUtils.compareMatrices(dmlfile, rfile, epsilon, "A-DML", "A-R");
			checkDMLMetaDataFile("A", new MatrixCharacteristics(rows,cols,1,1));
		}
		finally
		{
			rtplatform = platformOld;
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
		}
	}
}

