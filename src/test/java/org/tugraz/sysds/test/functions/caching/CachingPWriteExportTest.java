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

package org.tugraz.sysds.test.functions.caching;

import org.junit.Assert;
import org.junit.Test;
import org.tugraz.sysds.hops.Hop;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.runtime.matrix.data.InputInfo;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;

public class CachingPWriteExportTest extends AutomatedTestBase 
{
	
	private final static String TEST_NAME = "export";
	private final static String TEST_DIR = "functions/caching/";
	private final static String TEST_CLASS_DIR = TEST_DIR + CachingPWriteExportTest.class.getSimpleName() + "/";

	private final static int rows = (int)Hop.CPThreshold-1;
	private final static int cols = (int)Hop.CPThreshold-1;    
	private final static double sparsity = 0.7;
	
	@Override
	public void setUp() 
	{
		addTestConfiguration(TEST_NAME, 
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] { "V" }) ); 
	}
	
	@Test
	public void testExportReadWrite() 
	{
		runTestExport( "binary" );
	}
	
	@Test
	public void testExportCopy() 
	{
		runTestExport( "text" );
	}
	
	
	private void runTestExport( String outputFormat )
	{
		TestConfiguration config = getTestConfiguration(TEST_NAME);
		config.addVariable("rows", rows);
		config.addVariable("cols", cols);
		loadTestConfiguration(config);
		
		String HOME = SCRIPT_DIR + TEST_DIR;
		fullDMLScriptName = HOME + TEST_NAME + ".dml";
		programArgs = new String[]{"-args", input("V"),
			Integer.toString(rows), Integer.toString(cols), output("V"), outputFormat };

		long seed = System.nanoTime();
		long nnz = (long)Math.round(sparsity * rows * cols);
		double[][] V = getRandomMatrix(rows, cols, 0, 1, sparsity, seed);
		writeInputMatrix("V", V, true); //always text
		writeExpectedMatrix("V", V);
		
		boolean exceptionExpected = false;
		runTest(true, exceptionExpected, null, -1);
		
		double[][] Vp = null;
		try
		{
			InputInfo ii = null;
			if( outputFormat.equals("binary") )
				ii = InputInfo.BinaryBlockInputInfo;
			else
				ii = InputInfo.TextCellInputInfo;
			
			MatrixBlock mb = DataConverter.readMatrixFromHDFS(output("V"),
				ii, rows, cols, OptimizerUtils.DEFAULT_BLOCKSIZE, OptimizerUtils.DEFAULT_BLOCKSIZE, nnz);
			Vp = DataConverter.convertToDoubleMatrix(mb);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			throw new RuntimeException(ex);
		}
		
		//compare
		for( int i=0; i<rows; i++ )
			for( int j=0; j<cols; j++ )
				if( V[i][j]!=Vp[i][j] )
					Assert.fail("Wrong value i="+i+", j="+j+", value1="+V[i][j]+", value2="+Vp[i][j]);
	}
}