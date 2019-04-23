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

package org.tugraz.sysds.test.functions.binary.matrix_full_other;

import java.io.IOException;
import java.util.HashMap;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.common.Types.DataType;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.runtime.meta.MatrixCharacteristics;
import org.tugraz.sysds.runtime.util.HDFSTool;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class FullIntegerDivisionTest extends AutomatedTestBase 
{	
	private final static String TEST_NAME1 = "IntegerDivision_mod";
	private final static String TEST_NAME2 = "IntegerDivision_div";
	
	private final static String TEST_DIR = "functions/binary/matrix_full_other/";
	private final static String TEST_CLASS_DIR = TEST_DIR + FullIntegerDivisionTest.class.getSimpleName() + "/";
	private final static double eps = 1e-10;
	
	private final static int rows = 1100;
	private final static int cols = 900;
	private final static double sparsity1 = 0.7;
	private final static double sparsity2 = 0.1;
	
	private final static double min = 1.0;
	private final static double max = 100.0;
	
	
	private enum OpType{
		MOD,
		DIV,
	}
	
	
	@Override
	public void setUp() 
	{
		addTestConfiguration(TEST_NAME1,new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1,new String[]{"C"})); 
		addTestConfiguration(TEST_NAME2,new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2,new String[]{"C"}));
		if (TEST_CACHE_ENABLED) {
			setOutAndExpectedDeletionDisabled(true);
		}
	}

	@BeforeClass
	public static void init()
	{
		TestUtils.clearDirectory(TEST_DATA_DIR + TEST_CLASS_DIR);
	}

	@AfterClass
	public static void cleanUp()
	{
		if (TEST_CACHE_ENABLED) {
			TestUtils.clearDirectory(TEST_DATA_DIR + TEST_CLASS_DIR);
		}
	}

	//MOD
	
	@Test
	public void testModMMDenseCP() 
	{
		runIntegerDivisionTest(OpType.MOD, DataType.MATRIX, DataType.MATRIX, false, ExecType.CP);
	}
	
	
	
	@Test
	public void testModMSDenseCP() 
	{
		runIntegerDivisionTest(OpType.MOD, DataType.MATRIX, DataType.SCALAR, false, ExecType.CP);
	}
	
	@Test
	public void testModSMDenseCP() 
	{
		runIntegerDivisionTest(OpType.MOD, DataType.SCALAR, DataType.MATRIX, false, ExecType.CP);
	}
	
	@Test
	public void testModSSDenseCP() 
	{
		runIntegerDivisionTest(OpType.MOD, DataType.SCALAR, DataType.SCALAR, false, ExecType.CP);
	}
	
	@Test
	public void testModMMSparseCP() 
	{
		runIntegerDivisionTest(OpType.MOD, DataType.MATRIX, DataType.MATRIX, true, ExecType.CP);
	}
	
	@Test
	public void testModMSSparseCP() 
	{
		runIntegerDivisionTest(OpType.MOD, DataType.MATRIX, DataType.SCALAR, true, ExecType.CP);
	}
	
	@Test
	public void testModSMSparseCP() 
	{
		runIntegerDivisionTest(OpType.MOD, DataType.SCALAR, DataType.MATRIX, true, ExecType.CP);
	}
	
	@Test
	public void testModSSSparseCP() 
	{
		runIntegerDivisionTest(OpType.MOD, DataType.SCALAR, DataType.SCALAR, true, ExecType.CP);
	}

		
	
	//DIV

	@Test
	public void testDivMMDenseCP() 
	{
		runIntegerDivisionTest(OpType.DIV, DataType.MATRIX, DataType.MATRIX, false, ExecType.CP);
	}
	
	@Test
	public void testDivMSDenseCP() 
	{
		runIntegerDivisionTest(OpType.DIV, DataType.MATRIX, DataType.SCALAR, false, ExecType.CP);
	}
	
	@Test
	public void testDivSMDenseCP() 
	{
		runIntegerDivisionTest(OpType.DIV, DataType.SCALAR, DataType.MATRIX, false, ExecType.CP);
	}
	
	@Test
	public void testDivSSDenseCP() 
	{
		runIntegerDivisionTest(OpType.DIV, DataType.SCALAR, DataType.SCALAR, false, ExecType.CP);
	}
	
	@Test
	public void testDivMMSparseCP() 
	{
		runIntegerDivisionTest(OpType.DIV, DataType.MATRIX, DataType.MATRIX, true, ExecType.CP);
	}
	
	@Test
	public void testDivMSSparseCP() 
	{
		runIntegerDivisionTest(OpType.DIV, DataType.MATRIX, DataType.SCALAR, true, ExecType.CP);
	}
	
	@Test
	public void testDivSMSparseCP() 
	{
		runIntegerDivisionTest(OpType.DIV, DataType.SCALAR, DataType.MATRIX, true, ExecType.CP);
	}
	
	@Test
	public void testDivSSSparseCP() 
	{
		runIntegerDivisionTest(OpType.DIV, DataType.SCALAR, DataType.SCALAR, true, ExecType.CP);
	}

	
	private void runIntegerDivisionTest( OpType type, DataType dt1, DataType dt2, boolean sparse, ExecType instType)
	{
		//rtplatform for MR
		ExecMode platformOld = rtplatform;
		rtplatform = ExecMode.HYBRID;
	
		double sparsity = sparse?sparsity2:sparsity1;
		
		String TEST_CACHE_DIR = "";
		if (TEST_CACHE_ENABLED)
		{
			double sparsityLeft = 1.0;
			if (dt1 == DataType.MATRIX)
			{
				sparsityLeft = sparsity;
			}
			double sparsityRight = 1.0;
			if (dt2 == DataType.MATRIX)
			{
				sparsityRight = sparsity;
			}
			TEST_CACHE_DIR = type.name() + sparsityLeft + "_" + sparsityRight + "/";
		}

		try
		{
			String TEST_NAME = null;
			switch( type )
			{
				case MOD: TEST_NAME = TEST_NAME1; break;
				case DIV: TEST_NAME = TEST_NAME2; break;
			}
			
			TestConfiguration config = getTestConfiguration(TEST_NAME);
			loadTestConfiguration(config, TEST_CACHE_DIR);
			
			/* This is for running the junit test the new way, i.e., construct the arguments directly */
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-args", input("A"), input("B"), output("C") };
			
			fullRScriptName = HOME + TEST_NAME + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " + expectedDir();
			
			if( dt1 == DataType.SCALAR && dt2 == DataType.SCALAR ) {
				// Clear OUT folder to prevent access denied errors running DML script
				// for tests testModSSDense, testDivSSDense, testModSSSparse, testDivSSSparse
				// due to setOutAndExpectedDeletionDisabled(true).
				TestUtils.clearDirectory(outputDir());
			}
	
			//generate dataset A
			if( dt1 == DataType.MATRIX ){
				double[][] A = getRandomMatrix(rows, cols, min, max, sparsity, 7); 
				MatrixCharacteristics mcA = new MatrixCharacteristics(rows, cols, OptimizerUtils.DEFAULT_BLOCKSIZE, OptimizerUtils.DEFAULT_BLOCKSIZE, (long) (rows*cols*sparsity));
				writeInputMatrixWithMTD("A", A, true, mcA);
			}
			else{
				double[][] A = getRandomMatrix(1, 1, min, max, 1.0, 7);
				writeScalarInputMatrixWithMTD( "A", A, true );
			}
			
			//generate dataset B
			if( dt2 == DataType.MATRIX ){
				MatrixCharacteristics mcB = new MatrixCharacteristics(rows, cols, OptimizerUtils.DEFAULT_BLOCKSIZE, OptimizerUtils.DEFAULT_BLOCKSIZE, (long) (rows*cols*sparsity));
				double[][] B = getRandomMatrix(rows, cols, min, max, sparsity, 3); 
				writeInputMatrixWithMTD("B", B, true, mcB);
			}
			else{
				double[][] B = getRandomMatrix(1, 1, min, max, 1.0, 3);
				writeScalarInputMatrixWithMTD( "B", B, true );
			}
			boolean exceptionExpected = false;
			runTest(true, exceptionExpected, null, -1); 
			
			runRScript(true); 
		
			//compare matrices 
			HashMap<CellIndex, Double> dmlfile = null;
			HashMap<CellIndex, Double> rfile = readRMatrixFromFS("C");
			if( dt1==DataType.SCALAR&&dt2==DataType.SCALAR )
				dmlfile = readScalarMatrixFromHDFS("C");
			else
				dmlfile = readDMLMatrixFromHDFS("C");
			
			//NaN and Infinity currently ignored because R's writeMM replaces them with 1.0E308
			TestUtils.compareMatrices(dmlfile, rfile, eps, "Stat-DML", "Stat-R", true);
		}
		finally
		{
			rtplatform = platformOld;
		}
	}
	
	private static void writeScalarInputMatrixWithMTD(String name, double[][] matrix, boolean includeR) 
	{
		try {
			//write DML scalar
			String fname = baseDirectory + INPUT_DIR + name; // + "/in";
			HDFSTool.deleteFileIfExistOnHDFS(fname);
			HDFSTool.writeDoubleToHDFS(matrix[0][0], fname);
			HDFSTool.writeScalarMetaDataFile(baseDirectory + INPUT_DIR + name + ".mtd", ValueType.FP64);
		
			//write R matrix
			if( includeR ){
				String completeRPath = baseDirectory + INPUT_DIR + name + ".mtx";
				TestUtils.writeTestMatrix(completeRPath, matrix, true);
			}
		}
		catch(IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	private static HashMap<CellIndex,Double> readScalarMatrixFromHDFS(String name) {
		HashMap<CellIndex,Double> dmlfile = new HashMap<CellIndex,Double>();
		try {
			Double val = HDFSTool.readDoubleFromHDFSFile(baseDirectory + OUTPUT_DIR + name);
			dmlfile.put(new CellIndex(1,1), val);
		}
		catch(IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		return dmlfile;
	}
}