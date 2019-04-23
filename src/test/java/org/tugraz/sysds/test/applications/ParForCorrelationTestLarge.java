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
import org.tugraz.sysds.hops.Hop;
import org.tugraz.sysds.runtime.controlprogram.ParForProgramBlock.PExecMode;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

/**
 * Intension is to test file-based result merge with regard to its integration
 * with the different execution modes. Hence we need at least a dataset of size
 * CPThreshold^2
 * 
 * 
 */
public class ParForCorrelationTestLarge extends AutomatedTestBase 
{
	
	private final static String TEST_NAME = "parfor_corr_large";
	private final static String TEST_DIR = "applications/parfor/";
	private final static String TEST_CLASS_DIR = TEST_DIR + ParForCorrelationTestLarge.class.getSimpleName() + "/";
	private final static double eps = 1e-10;
	
	private final static int rows = (int)Hop.CPThreshold+1;  // # of rows in each vector (for MR instructions)
	private final static int cols = (int)Hop.CPThreshold+1;      // # of columns in each vector  
	
	private final static double minVal=0;    // minimum value in each vector 
	private final static double maxVal=1000; // maximum value in each vector 

	
	@Override
	public void setUp() 
	{
		addTestConfiguration(TEST_NAME,
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] { "Rout" }) );
	}

	@Test
	public void testParForCorrleationLargeLocalLocal() {
		runParForCorrelationTest(PExecMode.LOCAL, PExecMode.LOCAL);
	}

	@Test
	public void testParForCorrleationLargeDefault() {
		runParForCorrelationTest(null, null);
	}
	
	private void runParForCorrelationTest( PExecMode outer, PExecMode inner )
	{
		//script
		int scriptNum = -1;
		if( inner == PExecMode.REMOTE_SPARK )      scriptNum=2;
		else if( outer == PExecMode.REMOTE_SPARK ) scriptNum=3;
		else if( outer == PExecMode.LOCAL )     scriptNum=1;
		else                                    scriptNum=4; //optimized
		
		TestConfiguration config = getTestConfiguration(TEST_NAME);
		config.addVariable("rows", rows);
		config.addVariable("cols", cols);
		loadTestConfiguration(config);
		
		/* This is for running the junit test the new way, i.e., construct the arguments directly */
		String HOME = SCRIPT_DIR + TEST_DIR;
		fullDMLScriptName = HOME + TEST_NAME +scriptNum + ".dml";
		programArgs = new String[]{"-args", input("V"),
			Integer.toString(rows), Integer.toString(cols), output("PearsonR") };
		
		fullRScriptName = HOME + TEST_NAME + ".R";
		rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " + expectedDir();

		long seed = System.nanoTime();
		double[][] V = getRandomMatrix(rows, cols, minVal, maxVal, 1.0, seed);
		writeInputMatrix("V", V, true);

		boolean exceptionExpected = false;
		runTest(true, exceptionExpected, null, -1);
		runRScript(true);
		
		//compare matrices
		HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("PearsonR");
		HashMap<CellIndex, Double> rfile  = readRMatrixFromFS("Rout");
		TestUtils.compareMatrices(dmlfile, rfile, eps, "PearsonR-DML", "PearsonR-R");
	}
}