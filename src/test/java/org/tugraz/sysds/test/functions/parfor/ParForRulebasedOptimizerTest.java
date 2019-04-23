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

import org.junit.Test;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class ParForRulebasedOptimizerTest extends AutomatedTestBase 
{
	private final static String TEST_NAME1 = "parfor_optimizer1"; //+b for dml 
	private final static String TEST_NAME2 = "parfor_optimizer2"; //+b for dml
	private final static String TEST_NAME3 = "parfor_optimizer3"; //+b for dml
	private final static String TEST_DIR = "functions/parfor/";
	private final static String TEST_CLASS_DIR = TEST_DIR + ParForRulebasedOptimizerTest.class.getSimpleName() + "/";
	private final static double eps = 1e-10;
	
	private final static int rows1 = 1000; //small CP
	private final static int rows2 = 10000; //large MR
	
	private final static int cols11 = 50;  //small single parfor
	private final static int cols12 = 500; //large single parfor	
	
	private final static int cols21 = 5;  //small nested parfor
	private final static int cols22 = 50; //large nested parfor
	private final static int cols31 = 2;  //small nested parfor
	private final static int cols32 = 8; //large nested parfor
	private final static double sparsity = 0.7;
	
	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME1,
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] { "Rout" }) );
		addTestConfiguration(TEST_NAME2,
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2, new String[] { "Rout" }) );
		addTestConfiguration(TEST_NAME3,
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME3, new String[] { "Rout" }) );
	}

	@Test
	public void testParForRulebasedOptimizerCorrelationSmallSmall() {
		runParForOptimizerTest(1, false, false, false);
	}
	
	@Test
	public void testParForRulebasedOptimizerCorrelationSmallLarge() {
		runParForOptimizerTest(1, false, true, false);
	}
	
	@Test
	public void testParForRulebasedOptimizerCorrelationLargeSmall() {
		runParForOptimizerTest(1, true, false, false);
	}
	
	@Test
	public void testParForRulebasedOptimizerCorrelationLargeLarge() {
		runParForOptimizerTest(1, true, true, false);
	}
	
	@Test
	public void testParForRulebasedOptimizerBivariateStatsSmallSmall() {
		runParForOptimizerTest(2, false, false, false);
	}
	
	@Test
	public void testParForRulebasedOptimizerBivariateStatsSmallLarge() {
		runParForOptimizerTest(2, false, true, false);
	}
	
	@Test
	public void testParForRulebasedOptimizerBivariateStatsLargeSmall() {
		runParForOptimizerTest(2, true, false, false);
	}
	
	@Test
	public void testParForRulebasedOptimizerBivariateStatsLargeLarge() {
		runParForOptimizerTest(2, true, true, false);
	}
	
	@Test
	public void testParForRulebasedOptimizerFunctionInvocationSmallSmall() {
		runParForOptimizerTest(3, false, false, false);
	}
	
	@Test
	public void testParForRulebasedOptimizerFunctionInvocationSmallLarge() {
		runParForOptimizerTest(3, false, true, false);
	}
	
	@Test
	public void testParForRulebasedOptimizerFunctionInvocationLargeSmall() {
		runParForOptimizerTest(3, true, false, false);
	}
	
	@Test
	public void testParForRulebasedOptimizerFunctionInvocationLargeLarge() {
		runParForOptimizerTest(3, true, true, false);
	}
	
	@Test
	public void testParForHeuristicOptimizerCorrelationSmallSmall() {
		runParForOptimizerTest(1, false, false, true);
	}
	
	@Test
	public void testParForHeuristicOptimizerCorrelationSmallLarge() {
		runParForOptimizerTest(1, false, true, true);
	}
	
	@Test
	public void testParForHeuristicOptimizerCorrelationLargeSmall() {
		runParForOptimizerTest(1, true, false, true);
	}
	
	@Test
	public void testParForHeuristicOptimizerCorrelationLargeLarge() {
		runParForOptimizerTest(1, true, true, true);
	}
	
	@Test
	public void testParForHeuristicOptimizerBivariateStatsSmallSmall() {
		runParForOptimizerTest(2, false, false, true);
	}
	
	@Test
	public void testParForHeuristicOptimizerBivariateStatsSmallLarge() {
		runParForOptimizerTest(2, false, true, true);
	}
	
	@Test
	public void testParForHeuristicOptimizerBivariateStatsLargeSmall() {
		runParForOptimizerTest(2, true, false, true);
	}
	
	@Test
	public void testParForHeuristicOptimizerBivariateStatsLargeLarge() {
		runParForOptimizerTest(2, true, true, true);
	}
	
	@Test
	public void testParForHeuristicOptimizerFunctionInvocationSmallSmall() {
		runParForOptimizerTest(3, false, false, true);
	}
	
	@Test
	public void testParForHeuristicOptimizerFunctionInvocationSmallLarge() {
		runParForOptimizerTest(3, false, true, true);
	}
	
	@Test
	public void testParForHeuristicOptimizerFunctionInvocationLargeSmall() {
		runParForOptimizerTest(3, true, false, true);
	}
	
	@Test
	public void testParForHeuristicOptimizerFunctionInvocationLargeLarge() {
		runParForOptimizerTest(3, true, true, true);
	}
	
	
	private void runParForOptimizerTest( int scriptNum, boolean largeRows, boolean largeCols, boolean timebasedOpt )
	{
		//find right rows and cols configuration
		int rows=-1, cols=-1;
		if( largeRows )
			rows = rows2;
		else
			rows = rows1; 
		if( largeCols ){
			switch(scriptNum) {
				case 1: cols=cols22; break;
				case 2: cols=cols32; break;
				case 3: cols=cols12; break;
			}
		}
		else{
			switch(scriptNum) {
				case 1: cols=cols21; break;
				case 2: cols=cols31; break;
				case 3: cols=cols11; break;
			}
		}

		//run actual test
		switch( scriptNum ) {
			case 1: 
				runUnaryTest(scriptNum, timebasedOpt, rows, cols);
				break;
			case 2:
				runNaryTest(scriptNum, timebasedOpt, rows, cols);
				break;
			case 3: 
				runUnaryTest(scriptNum, timebasedOpt, rows, cols);
				break;
		}
	}
	
	private void runUnaryTest(int scriptNum, boolean timebasedOpt, int rows, int cols )
	{
		TestConfiguration config = null;
		String HOME = SCRIPT_DIR + TEST_DIR;
		if( scriptNum==1 ) {
			config=getTestConfiguration(TEST_NAME1);
			String testname = TEST_NAME1 + (timebasedOpt ? "b" : "");
			fullDMLScriptName = HOME + testname + ".dml";
		}
		else if( scriptNum==3 ) {
			config=getTestConfiguration(TEST_NAME3);
			String testname = TEST_NAME3 + (timebasedOpt ? "b" : "");
			fullDMLScriptName = HOME + testname + ".dml";
		}
		
		config.addVariable("rows", rows);
		config.addVariable("cols", cols);
		loadTestConfiguration(config);
		
		if( scriptNum==1 ) {
			programArgs = new String[]{ "-args", input("V"),
				Integer.toString(rows), Integer.toString(cols),
				output("R") };
			rCmd = "Rscript" + " " + HOME + TEST_NAME1 + ".R" + " " +
				inputDir() + " " + expectedDir();
		}
		else if( scriptNum==3 ) {
			programArgs = new String[]{ "-args", input("V"),
				Integer.toString(rows), Integer.toString(cols),
				Integer.toString(cols/2), 
				output("R") };
			rCmd = "Rscript" + " " + HOME + TEST_NAME3 + ".R" + " " +
				inputDir() + " " + expectedDir();
		}

		long seed = System.nanoTime();
		double[][] V = getRandomMatrix(rows, cols, 0, 1, sparsity, seed);
		writeInputMatrix("V", V, true);

		boolean exceptionExpected = false;
		runTest(true, exceptionExpected, null, -1);
	
		runRScript(true);
		
		//compare matrices
		HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("R");
		HashMap<CellIndex, Double> rfile  = readRMatrixFromFS("Rout");
		TestUtils.compareMatrices(dmlfile, rfile, eps, "DML", "R");
	}
	
	private void runNaryTest(int scriptNum, boolean timebasedOpt, int rows, int cols)
	{
		TestConfiguration config = getTestConfiguration(TEST_NAME2);
		config.addVariable("rows", rows);
		config.addVariable("cols", cols);
		loadTestConfiguration(config);
		
		/* This is for running the junit test the new way, i.e., construct the arguments directly */
		String HOME = SCRIPT_DIR + TEST_DIR;
		String testname = TEST_NAME2 + (timebasedOpt ? "b" : "");
		fullDMLScriptName = HOME + testname + ".dml";
		programArgs = new String[]{"-args", 
			input("D"),
			input("S1"), input("S2"),
			input("K1"), input("K2"),
			output("bivarstats"),
			Integer.toString(rows),
			Integer.toString(cols),
			Integer.toString(cols),
			Integer.toString(cols*cols),
			Integer.toString(7) };
			
		rCmd = "Rscript" + " " + HOME + TEST_NAME2 + ".R" + " " + 
			inputDir() + " " + Integer.toString(7) + " " + expectedDir();

		//generate actual dataset
		double[][] D = getRandomMatrix(rows, cols, 1, 7, 1.0, 7777); 
		double[] Dkind = new double[cols]; 
		for( int i=0; i<cols; i++ )
		{
			Dkind[i]=(i%3)+1;//kind 1,2,3
			if( Dkind[i]!=1 )
				TestUtils.floor(D,i); //for ordinal and categorical vars
		}
		writeInputMatrix("D", D, true);
		
		//generate attribute sets
		double[][] S1 = getRandomMatrix(1, cols, 1, cols+1-eps, 1, 1112);
		double[][] S2 = getRandomMatrix(1, cols, 1, cols+1-eps, 1, 1113);
		TestUtils.floor(S1);
		TestUtils.floor(S2);
		writeInputMatrix("S1", S1, true);
		writeInputMatrix("S2", S2, true);

		//generate kind for attributes (1,2,3)
		double[][] K1 = new double[1][cols];
		double[][] K2 = new double[1][cols];
		for( int i=0; i<cols; i++ ) {
			K1[0][i] = Dkind[(int)S1[0][i]-1];
			K2[0][i] = Dkind[(int)S2[0][i]-1];
		}
		writeInputMatrix("K1", K1, true);
		writeInputMatrix("K2", K2, true);
		
		boolean exceptionExpected = false;
		runTest(true, exceptionExpected, null, -1);

		runRScript(true);
		
		//compare matrices 
		for( String out : new String[]{"bivar.stats", "category.counts", "category.means",  "category.variances" } ) {
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("bivarstats/"+out);
			HashMap<CellIndex, Double> rfile  = readRMatrixFromFS(out);
			TestUtils.compareMatrices(dmlfile, rfile, eps, "Stat-DML", "Stat-R");
		}
	}
}