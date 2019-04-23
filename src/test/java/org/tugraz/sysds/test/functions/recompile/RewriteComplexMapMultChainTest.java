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

import java.io.IOException;
import java.util.HashMap;

import org.junit.Test;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class RewriteComplexMapMultChainTest extends AutomatedTestBase 
{
	
	private final static String TEST_NAME1 = "rewrite_mapmultchain1";
	private final static String TEST_NAME2 = "rewrite_mapmultchain2";
	private final static String TEST_DIR = "functions/recompile/";
	private final static String TEST_CLASS_DIR = TEST_DIR + 
		RewriteComplexMapMultChainTest.class.getSimpleName() + "/";
	
	private final static int rows = 1974;
	private final static int cols = 45;
	
	private final static int cols2a = 1;
	private final static int cols2b = 3;
	
	private final static double sparsity = 0.7;
	private final static double eps = 0.0000001;
	
	
	@Override
	public void setUp() 
	{
		addTestConfiguration( TEST_NAME1,
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] { "HV" }) );
		addTestConfiguration( TEST_NAME2,
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2, new String[] { "HV" }) );
	}
	
	
	@Test
	public void testRewriteExpr1SingleColumnCP() throws IOException {
		runRewriteMapMultChain(TEST_NAME1, true, ExecType.CP);
	}
	
	@Test
	public void testRewriteExpr1MultiColumnCP() throws IOException {
		runRewriteMapMultChain(TEST_NAME1, false, ExecType.CP);
	}
	
	@Test
	public void testRewriteExpr2SingleColumnCP() throws IOException {
		runRewriteMapMultChain(TEST_NAME2, true, ExecType.CP);
	}
	
	@Test
	public void testRewriteExpr2MultiColumnCP() throws IOException {
		runRewriteMapMultChain(TEST_NAME2, false, ExecType.CP);
	}
	
	private void runRewriteMapMultChain( String TEST_NAME, boolean singleCol, ExecType et ) throws IOException {
		ExecMode platformOld = rtplatform;
		
		try
		{
			TestConfiguration config = getTestConfiguration(TEST_NAME);
			loadTestConfiguration(config);
			
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-explain","-args",
				input("X"), input("P"), input("v"), output("HV") };
			
			fullRScriptName = HOME + TEST_NAME + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " + expectedDir();

			rtplatform = ExecMode.HYBRID;
			
			//generate input data
			double[][] X = getRandomMatrix(rows, cols, 0, 1, sparsity, 7);
			writeInputMatrixWithMTD("X", X, true);
			double[][] P = getRandomMatrix(rows, singleCol?cols2a:cols2b, 0, 1, sparsity, 3);
			writeInputMatrixWithMTD("P", P, true);
			double[][] v = getRandomMatrix(cols, singleCol?cols2a:cols2b, 0, 1, 1.0, 21);
			writeInputMatrixWithMTD("v", v, true);
			
			//run test
			runTest(true, false, null, -1); 
			runRScript(true);
			
			//compare matrices
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("HV");
			HashMap<CellIndex, Double> rfile  = readRMatrixFromFS("HV");
			TestUtils.compareMatrices(dmlfile, rfile, eps, "DML", "R");	
			
			//check expected number of compiled and executed MR jobs
			int expectedNumCompiled = (et==ExecType.CP)?1:(singleCol?4:6); //mapmultchain if single column
			int expectedNumExecuted = (et==ExecType.CP)?0:(singleCol?4:6); //mapmultchain if single column
			
			checkNumCompiledSparkInst(expectedNumCompiled); 
			checkNumExecutedSparkInst(expectedNumExecuted); 
		}
		finally
		{
			rtplatform = platformOld;
		}
	}
}
