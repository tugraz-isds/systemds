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

package org.tugraz.sysds.test.functions.parfor;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;
import org.tugraz.sysds.utils.Statistics;

public class ParForParallelRemoteResultMergeTest extends AutomatedTestBase 
{	
	private final static String TEST_NAME1 = "parfor_pr_resultmerge2";
	private final static String TEST_NAME2 = "parfor_pr_resultmerge32";
	private final static String TEST_DIR = "functions/parfor/";
	private final static String TEST_CLASS_DIR = TEST_DIR + ParForParallelRemoteResultMergeTest.class.getSimpleName() + "/";
	private final static double eps = 1e-10;
	
	private final static int rows = 1100;  
	private final static int cols = 70;  
	
	private final static double sparsity1 = 0.7;
	private final static double sparsity2 = 0.1d;
	
	
	@Override
	public void setUp() 
	{
		addTestConfiguration(TEST_NAME1, 
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] { "R" }) );
		addTestConfiguration(TEST_NAME2, 
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2, new String[] { "R" }) );
	}

	@Test
	public void testMultipleResultMergeFewDense() 
	{
		runParallelRemoteResultMerge(TEST_NAME1, false);
	}
	
	@Test
	public void testMultipleResultMergeFewSparse() 
	{
		runParallelRemoteResultMerge(TEST_NAME1, true);
	}
	
	@Test
	public void testMultipleResultMergeManyDense() 
	{
		runParallelRemoteResultMerge(TEST_NAME2, false);
	}
	
	@Test
	public void testMultipleResultMergeManySparse() 
	{
		runParallelRemoteResultMerge(TEST_NAME2, true);
	}
	
	
	/**
	 * 
	 * @param outer execution mode of outer parfor loop
	 * @param inner execution mode of inner parfor loop
	 * @param instType execution mode of instructions
	 */
	private void runParallelRemoteResultMerge( String test_name, boolean sparse )
	{
		//inst exec type, influenced via rows
		String TEST_NAME = test_name;
			
		//script
		TestConfiguration config = getTestConfiguration(TEST_NAME);
		config.addVariable("rows", rows);
		config.addVariable("cols", cols);
		loadTestConfiguration(config);
		
		/* This is for running the junit test the new way, i.e., construct the arguments directly */
		String HOME = SCRIPT_DIR + TEST_DIR;
		fullDMLScriptName = HOME + TEST_NAME + ".dml";
		programArgs = new String[]{"-args", input("V"), 
			Integer.toString(rows), Integer.toString(cols), output("R") };
		
		fullRScriptName = HOME + TEST_NAME + ".R";
		rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " + expectedDir();

		long seed = System.nanoTime();
		double sparsity = -1;
		if( sparse )
			sparsity = sparsity2;
		else
			sparsity = sparsity1;
        double[][] V = getRandomMatrix(rows, cols, 0, 1, sparsity, seed);
		writeInputMatrix("V", V, true);

		boolean exceptionExpected = false;
		runTest(true, exceptionExpected, null, -1);
		runRScript(true);
		
		//compare num MR jobs
		if( TEST_NAME.equals(TEST_NAME1) ) //2 results
			Assert.assertEquals("Unexpected number of executed MR jobs.",
				3, Statistics.getNoOfExecutedSPInst());
		else if ( TEST_NAME.equals(TEST_NAME2) ) //32 results
			Assert.assertEquals("Unexpected number of executed MR jobs.",
				33, Statistics.getNoOfExecutedSPInst());
		//compare matrices
		HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("R");
		HashMap<CellIndex, Double> rfile  = readRMatrixFromFS("Rout");
		TestUtils.compareMatrices(dmlfile, rfile, eps, "DML", "R");	
	}
}
