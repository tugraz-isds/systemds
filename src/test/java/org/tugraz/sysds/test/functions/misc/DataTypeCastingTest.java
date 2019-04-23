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

package org.tugraz.sysds.test.functions.misc;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.tugraz.sysds.api.DMLException;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.runtime.util.HDFSTool;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;

/**
 *   
 */
public class DataTypeCastingTest extends AutomatedTestBase
{
	
	private final static String TEST_DIR = "functions/misc/";
	private final static String TEST_CLASS_DIR = TEST_DIR + DataTypeCastingTest.class.getSimpleName() + "/";
	
	private final static String TEST_NAME1 = "castMatrixScalar";
	private final static String TEST_NAME2 = "castScalarMatrix";
	
	
	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] {"R"}));
		addTestConfiguration(TEST_NAME2, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2, new String[] {"R"}));
	}
	
	@Test
	public void testMatrixToScalar() 
	{ 
		runTest( TEST_NAME1, true, false ); 
	}
	
	@Test
	public void testMatrixToScalarWrongSize() 
	{ 
		runTest( TEST_NAME1, true, true ); 
	}
	
	@Test
	public void testScalarToScalar() 
	{ 
		runTest( TEST_NAME1, false, true ); 
	}
	
	@Test
	public void testScalarToMatrix() 
	{ 
		runTest( TEST_NAME2, false, false ); 
	}
	
	@Test
	public void testMatrixToMatrix() 
	{ 
		runTest( TEST_NAME2, true, true ); 
	}
	
	
	/**
	 * 
	 * @param cfc
	 * @param vt
	 */
	private void runTest( String testName, boolean matrixInput, boolean exceptionExpected ) 
	{
		String TEST_NAME = testName;
		int numVals = (exceptionExpected ? 7 : 1);
		
		try
		{		
			TestConfiguration config = getTestConfiguration(TEST_NAME);	
			loadTestConfiguration(config);
   
		    String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-args", input("V"), 
				Integer.toString(numVals), Integer.toString(numVals),
				output("R") };
			
			//write input
			double[][] V = getRandomMatrix(numVals, numVals, 0, 1, 1.0, 7);
			if( matrixInput ){
				writeInputMatrix("V", V, false);	
			}
			else{
				HDFSTool.writeDoubleToHDFS(V[0][0], input("V"));
				HDFSTool.writeScalarMetaDataFile(input("V.mtd"), ValueType.FP64);
			}
			
			//run tests
	        runTest(true, exceptionExpected, DMLException.class, -1);
	        
	        if( !exceptionExpected ){
		        //read output
		        double ret = -1;
		        if( testName.equals(TEST_NAME2) ){
		        	HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("R");
		        	ret = dmlfile.get(new CellIndex(1,1));		
		        }
				else if( testName.equals(TEST_NAME1) ){
					HashMap<CellIndex, Double> dmlfile = readDMLScalarFromHDFS("R");
					ret = dmlfile.get(new CellIndex(1,1));
				}
		        
		        //compare results
		        Assert.assertEquals(V[0][0], ret, 1e-16);
	        }
		}
		catch(Exception ex)
		{
			throw new RuntimeException(ex);
		}
	}
}
