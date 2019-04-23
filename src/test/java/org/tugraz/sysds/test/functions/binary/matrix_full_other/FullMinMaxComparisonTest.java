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
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.common.Types.DataType;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.matrix.data.OutputInfo;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.runtime.meta.MatrixCharacteristics;
import org.tugraz.sysds.runtime.util.HDFSTool;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class FullMinMaxComparisonTest extends AutomatedTestBase 
{
	
	private final static String TEST_NAME_R = "FullMinMaxComparison";
	private final static String TEST_NAME1 = "FullMinMaxComparison1";
	private final static String TEST_NAME2 = "FullMinMaxComparison2";
	private final static String TEST_NAME3 = "FullMinMaxComparison3";
	private final static String TEST_NAME4 = "FullMinMaxComparison4";
	
	private final static String TEST_DIR = "functions/binary/matrix_full_other/";
	private final static String TEST_CLASS_DIR = TEST_DIR + FullMinMaxComparisonTest.class.getSimpleName() + "/";
	private final static double eps = 1e-10;
	
	private final static int rows = 1782;
	private final static int cols = 34;
	private final static double sparsity1 = 0.7;
	private final static double sparsity2 = 0.1;
	
	private enum OpType{
		MIN,
		MAX
	}
	
	
	@Override
	public void setUp() 
	{
		addTestConfiguration(TEST_NAME1,new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1,new String[]{"C"})); 
		addTestConfiguration(TEST_NAME2,new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2,new String[]{"C"})); 
		addTestConfiguration(TEST_NAME3,new TestConfiguration(TEST_CLASS_DIR, TEST_NAME3,new String[]{"C"})); 
		addTestConfiguration(TEST_NAME4,new TestConfiguration(TEST_CLASS_DIR, TEST_NAME4,new String[]{"C"}));
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

	@Test
	public void testMinMatrixDenseMatrixDenseCP() 
	{
		runMinMaxComparisonTest(OpType.MIN, DataType.MATRIX, DataType.MATRIX, false, false, ExecType.CP);
	}
	
	@Test
	public void testMinMatrixDenseMatrixSparseCP() 
	{
		runMinMaxComparisonTest(OpType.MIN, DataType.MATRIX, DataType.MATRIX, false, true, ExecType.CP);
	}
	
	@Test
	public void testMinMatrixSparseMatrixDenseCP() 
	{
		runMinMaxComparisonTest(OpType.MIN, DataType.MATRIX, DataType.MATRIX, true, false, ExecType.CP);
	}
	
	@Test
	public void testMinMatrixSparseMatrixSparseCP() 
	{
		runMinMaxComparisonTest(OpType.MIN, DataType.MATRIX, DataType.MATRIX, true, true, ExecType.CP);
	}
	
	@Test
	public void testMinMatrixDenseScalarCP() 
	{
		runMinMaxComparisonTest(OpType.MIN, DataType.MATRIX, DataType.SCALAR, false, false, ExecType.CP);
	}
	
	@Test
	public void testMinMatrixSparseScalarCP() 
	{
		runMinMaxComparisonTest(OpType.MIN, DataType.MATRIX, DataType.SCALAR, true, false, ExecType.CP);
	}
	
	@Test
	public void testMinScalarMatrixDenseCP() 
	{
		runMinMaxComparisonTest(OpType.MIN, DataType.SCALAR, DataType.MATRIX, false, false, ExecType.CP);
	}
	
	@Test
	public void testMinScalarMatrixSparseCP() 
	{
		runMinMaxComparisonTest(OpType.MIN, DataType.SCALAR, DataType.MATRIX, false, true, ExecType.CP);
	}
	
	@Test
	public void testMinScalarScalarCP() 
	{
		runMinMaxComparisonTest(OpType.MIN, DataType.SCALAR, DataType.SCALAR, false, false, ExecType.CP);
	}
	
	@Test
	public void testMaxMatrixDenseMatrixDenseCP() 
	{
		runMinMaxComparisonTest(OpType.MAX, DataType.MATRIX, DataType.MATRIX, false, false, ExecType.CP);
	}
	
	@Test
	public void testMaxMatrixDenseMatrixSparseCP() 
	{
		runMinMaxComparisonTest(OpType.MAX, DataType.MATRIX, DataType.MATRIX, false, true, ExecType.CP);
	}
	
	@Test
	public void testMaxMatrixSparseMatrixDenseCP() 
	{
		runMinMaxComparisonTest(OpType.MAX, DataType.MATRIX, DataType.MATRIX, true, false, ExecType.CP);
	}
	
	@Test
	public void testMaxMatrixSparseMatrixSparseCP() 
	{
		runMinMaxComparisonTest(OpType.MAX, DataType.MATRIX, DataType.MATRIX, true, true, ExecType.CP);
	}
	
	@Test
	public void testMaxMatrixDenseScalarCP() 
	{
		runMinMaxComparisonTest(OpType.MAX, DataType.MATRIX, DataType.SCALAR, false, false, ExecType.CP);
	}
	
	@Test
	public void testMaxMatrixSparseScalarCP() 
	{
		runMinMaxComparisonTest(OpType.MAX, DataType.MATRIX, DataType.SCALAR, true, false, ExecType.CP);
	}
	
	@Test
	public void testMaxScalarMatrixDenseCP() 
	{
		runMinMaxComparisonTest(OpType.MAX, DataType.SCALAR, DataType.MATRIX, false, false, ExecType.CP);
	}
	
	@Test
	public void testMaxScalarMatrixSparseCP() 
	{
		runMinMaxComparisonTest(OpType.MAX, DataType.SCALAR, DataType.MATRIX, false, true, ExecType.CP);
	}
	
	@Test
	public void testMaxScalarScalarCP() 
	{
		runMinMaxComparisonTest(OpType.MAX, DataType.SCALAR, DataType.SCALAR, false, false, ExecType.CP);
	}
	
	private void runMinMaxComparisonTest( OpType type, DataType dtM1, DataType dtM2, boolean sparseM1, boolean sparseM2, ExecType instType)
	{
		//rtplatform for MR
		ExecMode platformOld = rtplatform;
		rtplatform = ExecMode.HYBRID;
	
		//get the testname
		String TEST_NAME = null;
		int minFlag = (type==OpType.MIN)?1:0;
		boolean s1Flag = (dtM1==DataType.SCALAR);
		boolean s2Flag = (dtM2==DataType.SCALAR);
		
		if( s1Flag && s2Flag )
			TEST_NAME = TEST_NAME4;
		else if( s1Flag )
			TEST_NAME = TEST_NAME2;
		else if( s2Flag )	
			TEST_NAME = TEST_NAME3;
		else 
			TEST_NAME = TEST_NAME1;

		String TEST_CACHE_DIR = "";
		if (TEST_CACHE_ENABLED)
		{
			int mrows1 = (dtM1==DataType.MATRIX)? rows:1;
			int mrows2 = (dtM2==DataType.MATRIX)? rows:1;
			
			double sparsityLeft = sparseM1?sparsity2:sparsity1;
			double sparsityRight = sparseM2?sparsity2:sparsity1;

			TEST_CACHE_DIR = minFlag + "_" + mrows1 + "_" + mrows2 + "_" + sparsityLeft + "_" + sparsityRight + "/";
		}

		try
		{
			TestConfiguration config = getTestConfiguration(TEST_NAME);
			loadTestConfiguration(config, TEST_CACHE_DIR);
			
			// This is for running the junit test the new way, i.e., construct the arguments directly
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-explain","-args", input("A"), input("B"), 
				Integer.toString(minFlag), output("C") };
			
			fullRScriptName = HOME + TEST_NAME_R + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " + minFlag + " " + expectedDir();
	
			//generate actual dataset
			int mrows1 = (dtM1==DataType.MATRIX)? rows:1;
			int mcols1 = (dtM1==DataType.MATRIX)? cols:1;
			int mrows2 = (dtM2==DataType.MATRIX)? rows:1;
			int mcols2 = (dtM2==DataType.MATRIX)? cols:1;
			double[][] A = getRandomMatrix(mrows1, mcols1, -1, 1, sparseM1?sparsity2:sparsity1, 7); 
			writeInputMatrix("A", A, true);
			MatrixCharacteristics mc1 = new MatrixCharacteristics(mrows1,mcols1,1000,1000);
			HDFSTool.writeMetaDataFile(input("A.mtd"), ValueType.FP64, mc1, OutputInfo.TextCellOutputInfo);
			
			double[][] B = getRandomMatrix(mrows2, mcols2, -1, 1, sparseM2?sparsity2:sparsity1, 3); 
			writeInputMatrix("B", B, true);
			MatrixCharacteristics mc2 = new MatrixCharacteristics(mrows2,mcols2,1000,1000);
			HDFSTool.writeMetaDataFile(input("B.mtd"), ValueType.FP64, mc2, OutputInfo.TextCellOutputInfo);
			
			//run test
			runTest(true, false, null, -1); 
			runRScript(true); 
			
			//compare matrices 
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("C");
			HashMap<CellIndex, Double> rfile  = readRMatrixFromFS("C");
			TestUtils.compareMatrices(dmlfile, rfile, eps, "Stat-DML", "Stat-R");		
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		finally
		{
			rtplatform = platformOld;
		}
	}
}