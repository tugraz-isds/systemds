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

package org.tugraz.sysds.test.applications;

import java.util.HashMap;

import org.junit.Test;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.runtime.controlprogram.ParForProgramBlock.PExecMode;
import org.tugraz.sysds.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.runtime.meta.MatrixCharacteristics;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class ParForNaiveBayesTest extends AutomatedTestBase 
{
	private final static String TEST_NAME = "parfor_naive-bayes";
	private final static String TEST_DIR = "applications/parfor/";
	private final static String TEST_CLASS_DIR = TEST_DIR + ParForNaiveBayesTest.class.getSimpleName() + "/";
	private final static double eps = 1e-10; 
	
	private final static int rows = 50000;
	private final static int cols1 = 105;      // # of columns in each vector
	
	private final static double minVal=1;    // minimum value in each vector 
	private final static double maxVal=5; // maximum value in each vector 

	private final static double sparsity1 = 0.7;
	private final static double sparsity2 = 0.05;
	
	
	@Override
	public void setUp() 
	{
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, 
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, 
				new String[] { "class_prior", "class_conditionals" }) );
	}
	
	@Test
	public void testParForNaiveBayesLocalDenseCP() {
		runParForNaiveBayesTest(PExecMode.LOCAL, ExecType.CP, false, false);
	}
	
	@Test
	public void testParForNaiveBayesLocalSparseCP() {
		runParForNaiveBayesTest(PExecMode.LOCAL, ExecType.CP, false, true);
	}
	
	@Test
	public void testParForNaiveBayesRemoteDenseCP() {
		runParForNaiveBayesTest(PExecMode.REMOTE_SPARK, ExecType.CP, false, false);
	}
	
	@Test
	public void testParForNaiveBayesRemoteSparseCP() {
		runParForNaiveBayesTest(PExecMode.REMOTE_SPARK, ExecType.CP, false, true);
	}
	
	@Test
	public void testParForNaiveBayesDefaultDenseCP() {
		runParForNaiveBayesTest(null, ExecType.CP, false, false);
	}
	
	@Test
	public void testParForNaiveBayesDefaultSparseCP() {
		runParForNaiveBayesTest(null, ExecType.CP, false, true);
	}
	
	@Test
	public void testParForNaiveBayesRemoteDPDenseCPSmallMem() {
		runParForNaiveBayesTest(PExecMode.REMOTE_SPARK_DP, ExecType.CP, true, false);
	}
	
	@Test
	public void testParForNaiveBayesRemoteDPSparseCPSmallMem() {
		runParForNaiveBayesTest(PExecMode.REMOTE_SPARK_DP, ExecType.CP, true, true);
	}
	
	private void runParForNaiveBayesTest( PExecMode outer, ExecType instType, boolean smallMem, boolean sparse )
	{
		int cols = cols1;
		
		//inst exec type, influenced via rows
		ExecMode oldPlatform = rtplatform;
		rtplatform = ExecMode.HYBRID;
		
		//determine the script
		int scriptNum = -1;
		if( outer == PExecMode.LOCAL )      scriptNum=1; //constrained opt
		else if( outer == PExecMode.REMOTE_SPARK ) scriptNum=2; //constrained opt
		else if( outer == PExecMode.REMOTE_SPARK_DP ) 	scriptNum=3; //constrained opt
		else                                    scriptNum=4; //opt
	
		//invocation arguments
		TestConfiguration config = getTestConfiguration(TEST_NAME);
		config.addVariable("rows", rows);
		config.addVariable("cols", cols);
		loadTestConfiguration(config);
		
		String HOME = SCRIPT_DIR + TEST_DIR;
		fullDMLScriptName = HOME + TEST_NAME + scriptNum + ".dml";
		programArgs = new String[]{"-args", input("D"), input("C"), Integer.toString((int)maxVal),
			output("class_prior"), output("class_conditionals")};
		
		fullRScriptName = HOME + TEST_NAME + ".R";
		rCmd = "Rscript" + " " + fullRScriptName + " " + 
			inputDir() + " " + Integer.toString((int)maxVal) + " " + expectedDir();

		//input data
		double[][] D = getRandomMatrix(rows, cols, -1, 1, sparse?sparsity2:sparsity1, 7); 
		double[][] C = TestUtils.round(getRandomMatrix(rows, 1, minVal, maxVal, 1.0, 3)); 
		MatrixCharacteristics mc1 = new MatrixCharacteristics(rows,cols,-1,-1);
		writeInputMatrixWithMTD("D", D, true, mc1);
		MatrixCharacteristics mc2 = new MatrixCharacteristics(rows,1,-1,-1);
		writeInputMatrixWithMTD("C", C, true, mc2);
		
		//set memory budget (to test automatic opt for remote_mr_dp)
		long oldmem = InfrastructureAnalyzer.getLocalMaxMemory();
		if(smallMem) {
			long mem = 1024*1024*8;
			InfrastructureAnalyzer.setLocalMaxMemory(mem);
		}
		
		try
		{
			//run the testcase (DML and R)
			runTest(true, false, null, -1);
			runRScript(true); 
				
			//compare output matrices
			for( String out : new String[]{"class_prior", "class_conditionals" } )
			{
				HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS(out);				
				HashMap<CellIndex, Double> rfile  = readRMatrixFromFS(out);
				TestUtils.compareMatrices(dmlfile, rfile, eps, "Stat-DML", "Stat-R");
			}	
		}
		finally
		{
			rtplatform = oldPlatform;
			InfrastructureAnalyzer.setLocalMaxMemory(oldmem);
		}
	}

}