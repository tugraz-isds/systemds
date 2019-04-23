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
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class ParForDataPartitionLeftIndexingTest extends AutomatedTestBase 
{
	
	private final static String TEST_NAME1 = "parfor_cdatapartition_leftindexing";
	private final static String TEST_NAME2 = "parfor_rdatapartition_leftindexing";
	private final static String TEST_DIR = "functions/parfor/";
	private final static String TEST_CLASS_DIR = TEST_DIR + ParForDataPartitionLeftIndexingTest.class.getSimpleName() + "/";
	private final static double eps = 1e-10;
	
	private final static long dim1 = 10000;
	private final static long dim2 = 16;
	
	private final static double sparsity1 = 0.7;
	private final static double sparsity2 = 0.1d;
	
	
	@Override
	public void setUp() 
	{
		addTestConfiguration(TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] { "R" }) );
		addTestConfiguration(TEST_NAME2, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2, new String[] { "R" }) );
	}

	//colwise partitioning
	
	@Test
	public void testParForDataPartitionLeftindexingColDenseCP() 
	{
		runParForDataPartitionLeftindexingTest(true, false, ExecType.CP);
	}
	
	@Test
	public void testParForDataPartitionLeftindexingColSparseCP() 
	{
		runParForDataPartitionLeftindexingTest(true, true, ExecType.CP);
	}
	
	@Test
	public void testParForDataPartitionLeftindexingRowDenseCP() 
	{
		runParForDataPartitionLeftindexingTest(false, false, ExecType.CP);
	}
	
	@Test
	public void testParForDataPartitionLeftindexingRowSparseCP() 
	{
		runParForDataPartitionLeftindexingTest(false, true, ExecType.CP);
	}

	private void runParForDataPartitionLeftindexingTest( boolean colwise, boolean sparse, ExecType et )
	{
		//inst exec type, influenced via rows
		long rows = -1, cols = -1;
		if( colwise ){
			rows = dim1;
			cols = dim2;
		}
		else{
			rows = dim2;
			cols = dim1;
		}
			
		//script
		String TEST_NAME = colwise ? TEST_NAME1 : TEST_NAME2;
		double sparsity = sparse ? sparsity2 : sparsity1;
		
		TestConfiguration config = getTestConfiguration(TEST_NAME);
		config.addVariable("rows", rows);
		config.addVariable("cols", cols);
		loadTestConfiguration(config);
		
		/* This is for running the junit test the new way, i.e., construct the arguments directly */
		String HOME = SCRIPT_DIR + TEST_DIR;
		fullDMLScriptName = HOME + TEST_NAME + ".dml";
		programArgs = new String[]{"-args", input("V"), output("R") };
		
		fullRScriptName = HOME + TEST_NAME + ".R";
		rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " + expectedDir();

		//generate input data
		double[][] V = getRandomMatrix((int)rows, (int)cols, 0, 1, sparsity, 7);
		writeInputMatrixWithMTD("V", V, false);

		//run tests
		runTest(true, false, null, -1);
		
		//compare matrices
		HashMap<CellIndex, Double> input = TestUtils.convert2DDoubleArrayToHashMap(V);
		HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("R");
		TestUtils.compareMatrices(dmlfile, input, eps, "DML", "Input");
	}
}