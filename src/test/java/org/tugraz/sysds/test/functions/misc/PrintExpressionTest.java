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
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

/**
 * 
 */
public class PrintExpressionTest extends AutomatedTestBase 
{
	private final static String TEST_NAME1 = "PrintExpressionTest1";
	private final static String TEST_NAME2 = "PrintExpressionTest2";
	private final static String TEST_DIR = "functions/misc/";
	private final static String TEST_CLASS_DIR = TEST_DIR + PrintExpressionTest.class.getSimpleName() + "/";
	
	@Override
	public void setUp() 
	{
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] { "R" })); 
		addTestConfiguration(TEST_NAME2, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2, new String[] { "R" })); 
	}
		
	@Test
	public void testPrintNotExpressionTest() {
		runPrintExpressionTest(TEST_NAME1, false);
	}
	
	@Test
	public void testPrintMinusExpressionTest() {
		runPrintExpressionTest(TEST_NAME2, false);
	}
	
	@Test
	public void testPrintNotExpressionTestRewrite() {
		runPrintExpressionTest(TEST_NAME1, true);
	}
	
	@Test
	public void testPrintMinusExpressionTestRewrite() {
		runPrintExpressionTest(TEST_NAME2, true);
	}
		
	/**
	 * 
	 * @param testname
	 * @param rewrites
	 */
	private void runPrintExpressionTest( String testname, boolean rewrites )
	{
		String TEST_NAME = testname;
		TestConfiguration config = getTestConfiguration(TEST_NAME);
		loadTestConfiguration(config);
		
		//set rewrite configuration
		boolean oldRewriteFlag = OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION;
		OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = rewrites;
		
		try
		{
			String HOME = SCRIPT_DIR + TEST_DIR;			
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-args", output("R")};
			
			fullRScriptName = HOME + TEST_NAME +".R";
			rCmd = getRCmd(expectedDir());
	
			//run Tests
			runTest(true, false, null, -1);
		}
		finally
		{
			OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = oldRewriteFlag;
		}
	}
}
