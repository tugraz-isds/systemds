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
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

/**
 *   
 */
public class LongOverflowTest extends AutomatedTestBase
{
	
	private final static String TEST_DIR = "functions/misc/";
	private final static String TEST_CLASS_DIR = TEST_DIR + LongOverflowTest.class.getSimpleName() + "/";

	private final static String TEST_NAME1 = "LongOverflowMult";
	private final static String TEST_NAME2 = "LongOverflowPlus";
	private final static String TEST_NAME3 = "LongOverflowForLoop";
	
	private final static long val1 = (long)Math.pow(2,33); // base operand
	private final static long val2 = 10;   // operand success
	private final static long val3 = (long)Math.pow(2,63); // operand error
	
	private final static long val4 = (long)Math.pow(2,33); // for loop end
	private final static long val5 = (long)Math.pow(2,33)-10000000; // for loop start
	
	
	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] {}));
		addTestConfiguration(TEST_NAME2, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2, new String[] {}));
		addTestConfiguration(TEST_NAME3, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME3, new String[] {}));
	}
	
	@Test
	public void testLongOverflowMultNoError() 
	{ 
		runOverflowTest( TEST_NAME1, false ); 
	}
	
	@Test
	public void testLongOverflowMultError() 
	{ 
		runOverflowTest( TEST_NAME1, true ); 
	}
	
	@Test
	public void testLongOverflowPlusNoError() 
	{ 
		runOverflowTest( TEST_NAME2, false ); 
	}
	
	@Test
	public void testLongOverflowPlusError() 
	{ 
		runOverflowTest( TEST_NAME2, true ); 
	}
	
	@Test
	public void testLongOverflowForNoError() 
	{ 
		runOverflowTest( TEST_NAME3, false ); 
	}
	
	
	/**
	 * 
	 * @param cfc
	 * @param vt
	 */
	private void runOverflowTest( String testscript, boolean error ) 
	{
		String TEST_NAME = testscript;
		
		try
		{		
			getAndLoadTestConfiguration(TEST_NAME);
		    
			//generate input data;
			long input1 = (TEST_NAME.equals(TEST_NAME3)? val5 : val1);
			long input2 = (TEST_NAME.equals(TEST_NAME3)? val4 : error ? val3 : val2 );
			
		    String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-args", Long.toString(input1), Long.toString(input2) };
			
			//run tests
	        runTest(true, error, DMLException.class, -1);
		}
		catch(Exception ex)
		{
			throw new RuntimeException(ex);
		}
	}
}
