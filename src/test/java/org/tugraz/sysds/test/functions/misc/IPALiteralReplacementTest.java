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

import org.junit.Test;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

/**
 * Regression test for function recompile-once issue with literal replacement.
 * 
 */
public class IPALiteralReplacementTest extends AutomatedTestBase 
{
	
	private final static String TEST_NAME1 = "IPALiteralReplacement_While";
	private final static String TEST_NAME2 = "IPALiteralReplacement_ForIf";
	private final static String TEST_DIR = "functions/misc/";
	private final static String TEST_CLASS_DIR = TEST_DIR + IPALiteralReplacementTest.class.getSimpleName() + "/";
	
	@Override
	public void setUp() 
	{
		TestUtils.clearAssertionInformation();
		addTestConfiguration( TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] { "R" })   );
		addTestConfiguration( TEST_NAME2, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2, new String[] { "R" })   );
	}

	@Test
	public void testUnknownRecursionWhileNoIPA() 
	{
		runIPALiteralReplacementTest( TEST_NAME1, false );
	}
	
	@Test
	public void testUnknownRecursionWhileIPA() 
	{
		runIPALiteralReplacementTest( TEST_NAME1, true );
	}
	
	@Test
	public void testUnknownRecursionForIfNoIPA() 
	{
		runIPALiteralReplacementTest( TEST_NAME2, false );
	}
	
	@Test
	public void testUnknownRecursionForIfIPA() 
	{
		runIPALiteralReplacementTest( TEST_NAME2, true );
	}
	
	/**
	 * 
	 * @param condition
	 * @param branchRemoval
	 * @param IPA
	 */
	private void runIPALiteralReplacementTest( String testname, boolean IPA )
	{	
		boolean oldFlagIPA = OptimizerUtils.ALLOW_INTER_PROCEDURAL_ANALYSIS;
		
		try
		{
			TestConfiguration config = getTestConfiguration(testname);
			loadTestConfiguration(config);
			
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + testname + ".dml";
			programArgs = new String[]{"-args", output("R") };
			fullRScriptName = HOME + testname + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + expectedDir();			

			OptimizerUtils.ALLOW_INTER_PROCEDURAL_ANALYSIS = IPA;

			runTest(true, false, null, -1); 
			runRScript(true); 
			
			//compare matrices 
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("R");
			HashMap<CellIndex, Double> rfile  = readRMatrixFromFS("R");
			TestUtils.compareMatrices(dmlfile, rfile, 0, "Stat-DML", "Stat-R");
		}
		finally
		{
			OptimizerUtils.ALLOW_INTER_PROCEDURAL_ANALYSIS = oldFlagIPA;
		}
	}	
}