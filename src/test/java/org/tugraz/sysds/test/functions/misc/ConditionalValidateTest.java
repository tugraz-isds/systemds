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

import org.junit.Test;
import org.tugraz.sysds.api.DMLException;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.data.OutputInfo;
import org.tugraz.sysds.runtime.meta.MatrixCharacteristics;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.runtime.util.HDFSTool;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

/**
 *   
 */
public class ConditionalValidateTest extends AutomatedTestBase
{
	
	private final static String TEST_DIR = "functions/misc/";
	private final static String TEST_CLASS_DIR = TEST_DIR + ConditionalValidateTest.class.getSimpleName() + "/";

	private final static String TEST_NAME1 = "conditionalValidate1"; //plain
	private final static String TEST_NAME2 = "conditionalValidate2"; //if
	private final static String TEST_NAME3 = "conditionalValidate3"; //for
	private final static String TEST_NAME4 = "conditionalValidate4"; //while
	
	
	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] {"R"}));
		addTestConfiguration(TEST_NAME2, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2, new String[] {"R"}));
		addTestConfiguration(TEST_NAME3, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME3, new String[] {"R"}));
		addTestConfiguration(TEST_NAME4, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME4, new String[] {"R"}));
	}
	
	@Test
	public void testUnconditionalReadNoError() 
	{ 
		runTest( TEST_NAME1, false, true ); 
	}
	
	@Test
	public void testUnconditionalReadError() 
	{ 
		runTest( TEST_NAME1, true, false ); 
	}
	
	@Test
	public void testIfConditionalReadNoErrorExists() 
	{ 
		runTest( TEST_NAME2, false, true ); 
	}
	
	@Test
	public void testIfConditionalReadNoErrorNotExists() 
	{ 
		runTest( TEST_NAME2, false, false ); 
	}
	
	@Test
	public void testForConditionalReadNoErrorExists() 
	{ 
		runTest( TEST_NAME3, false, true ); 
	}
	
	@Test
	public void testForConditionalReadNoErrorNotExists() 
	{ 
		runTest( TEST_NAME3, false, false ); 
	}
	
	@Test
	public void testWhileConditionalReadNoErrorExists() 
	{ 
		runTest( TEST_NAME4, false, true ); 
	}
	
	@Test
	public void testWhileConditionalReadNoErrorNotExists() 
	{ 
		runTest( TEST_NAME4, false, false ); 
	}
	
	
	/**
	 * 
	 * @param cfc
	 * @param vt
	 */
	private void runTest( String testName, boolean exceptionExpected, boolean fileExists ) 
	{
		String TEST_NAME = testName;

		try
		{		
			TestConfiguration config = getTestConfiguration(TEST_NAME);
			loadTestConfiguration(config);

		    String HOME = SCRIPT_DIR + TEST_DIR;
		    String input = input("Y");
			
		    fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-args", input };
			
			//write input
			double[][] Y = getRandomMatrix(10, 15, 0, 1, 1.0, 7);
			MatrixBlock mb = DataConverter.convertToMatrixBlock(Y);
			MatrixCharacteristics mc = new MatrixCharacteristics(10,15,1000,1000);
			
			DataConverter.writeMatrixToHDFS(mb, input+(fileExists?"":"b"), OutputInfo.TextCellOutputInfo, mc);
			HDFSTool.writeMetaDataFile(input+(fileExists?"":"b")+".mtd", ValueType.FP64, mc, OutputInfo.TextCellOutputInfo);
			
			//run tests
	        runTest(true, exceptionExpected, DMLException.class, -1);
	        
	        //cleanup
	        HDFSTool.deleteFileIfExistOnHDFS(input);
	        HDFSTool.deleteFileIfExistOnHDFS(input+"b");
	        HDFSTool.deleteFileIfExistOnHDFS(input+".mtd");
	        HDFSTool.deleteFileIfExistOnHDFS(input+"b.mtd");	        
		}
		catch(Exception ex)
		{
			throw new RuntimeException(ex);
		}
	}
}
