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

package org.tugraz.sysds.test.functions.data;


import java.util.HashMap;

import org.junit.Test;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;



/**
 * 
 */
public class RandVarMinMaxTest extends AutomatedTestBase 
{
	
	private final static String TEST_NAME_DML1 = "RandVarMinMax1";
	private final static String TEST_NAME_DML2 = "RandVarMinMax2";
	private final static String TEST_NAME_DML3 = "RandVarMinMax3";
	private final static String TEST_NAME_R = "RandVarMinMax";
	private final static String TEST_DIR = "functions/data/";
	private final static String TEST_CLASS_DIR = TEST_DIR + RandVarMinMaxTest.class.getSimpleName() + "/";
	
	private final static int rows = 2;
	private final static int cols = 100;
	
		
	@Override
	public void setUp() 
	{
		TestUtils.clearAssertionInformation();
		addTestConfiguration( TEST_NAME_DML1, 
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME_DML1, new String[] { "R" }) );
		addTestConfiguration( TEST_NAME_DML2, 
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME_DML2, new String[] { "R" }) );
		addTestConfiguration( TEST_NAME_DML3, 
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME_DML3, new String[] { "R" }) );
	}

	@Test
	public void testMatrixVarMinMaxCP() {
		runRandVarMinMaxTest(TEST_NAME_DML1, ExecType.CP);
	}
	
	@Test
	public void testRandVarMinMaxCP() {
		runRandVarMinMaxTest(TEST_NAME_DML2, ExecType.CP);
	}
	
	@Test
	public void testMatrixVarExpressionCP() {
		runRandVarMinMaxTest(TEST_NAME_DML3, ExecType.CP);
	}
	
	@Test
	public void testMatrixVarMinMaxSP() {
		runRandVarMinMaxTest(TEST_NAME_DML1, ExecType.SPARK);
	}
	
	@Test
	public void testRandVarMinMaxSP() {
		runRandVarMinMaxTest(TEST_NAME_DML2, ExecType.SPARK);
	}
	
	@Test
	public void testMatrixVarExpressionSP() {
		runRandVarMinMaxTest(TEST_NAME_DML3, ExecType.SPARK);
	}

	private void runRandVarMinMaxTest( String TEST_NAME, ExecType instType )
	{
		//rtplatform for MR
		ExecMode platformOld = rtplatform;
		switch( instType ){
			case SPARK: rtplatform = ExecMode.SPARK; break;
			default: rtplatform = ExecMode.HYBRID; break;
		}
		
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		if( rtplatform == ExecMode.SPARK )
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;
	
		try
		{
			TestConfiguration config = getTestConfiguration(TEST_NAME);
			loadTestConfiguration(config);
			
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullRScriptName = HOME + TEST_NAME_R + ".R";
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-args", 
					Integer.toString(rows), Integer.toString(cols), output("R") };
			
			rCmd = "Rscript" + " " + fullRScriptName + " " + 
					Integer.toString(rows) + " " + Integer.toString(cols) + " " + expectedDir();
	
			runTest(true, false, null, -1);
			runRScript(true); 
			
			//compare matrices 
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("R");
			HashMap<CellIndex, Double> rfile  = readRMatrixFromFS("R");
			TestUtils.compareMatrices(dmlfile, rfile, 0, "Stat-DML", "Stat-R");
		}
		finally
		{
			rtplatform = platformOld;
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
		}
	}
	
}
