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

package org.tugraz.sysds.test.functions.updateinplace;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class UpdateInPlaceTest extends AutomatedTestBase 
{
	
	private final static String TEST_DIR = "functions/updateinplace/";
	private final static String TEST_NAME = "updateinplace";
	private final static String TEST_CLASS_DIR = TEST_DIR + UpdateInPlaceTest.class.getSimpleName() + "/";
	
	/* Test cases to test following scenarios
	 * 
	 * Test scenarios										Test case
	 * ------------------------------------------------------------------------------------
	 * 
	 * Positive case::
	 * ===============
	 * 
	 * Candidate UIP applicable								testUIP
	 * 
	 * Interleave Operalap::
	 * =====================
	 * 
	 * Various loop types::
	 * --------------------
	 * 
	 * Overlap for Consumer	within while loop				testUIPNAConsUsed
	 * Overlap for Consumer	outside loop					testUIPNAConsUsedOutsideDAG
	 * Overlap for Consumer	within loop(not used)			testUIPNAConsUsed
	 * Overlap for Consumer	within for loop					testUIPNAConsUsedForLoop
	 * Overlap for Consumer	within inner parfor loop		testUIPNAParFor
	 * Overlap for Consumer	inside loop						testUIPNAConsUsedInsideDAG
	 * 
	 * Complex Statement:: 
	 * -------------------
	 * 
	 * Overlap for Consumer	within complex statement		testUIPNAComplexConsUsed
	 * 		(Consumer in complex statement)
	 * Overlap for Consumer	within complex statement		testUIPNAComplexCandUsed
	 * 		(Candidate in complex statement)
	 * 
	 * Else and Predicate case::
	 * -------------------------
	 * 
	 * Overlap for Consumer	within else clause				testUIPNAConsUsedElse
	 * Overlap with consumer in predicate					testUIPNACandInPredicate
	 * 
	 * Multiple LIX for same object with interleave::
	 * ----------------------------------------------
	 * 
	 * Overlap for Consumer	with multiple lix				testUIPNAMultiLIX
	 * 
	 * 
	 * Function Calls::
	 * ================ 
	 * 
	 * Overlap for candidate used in function call 			testUIPNACandInFuncCall
	 * Overlap for consumer used in function call 			testUIPNAConsInFuncCall
	 * Function call without consumer/candidate 			testUIPFuncCall
	 * 
	 */
	
	
	
	//Note: In order to run these tests against ParFor loop, parfor's DEBUG flag needs to be set in the script.
	
	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, null));
	}

	@Test
	public void testUIP() 
	{
		List<String> listUIPRes = Arrays.asList("A");

		runUpdateInPlaceTest(TEST_NAME, 1, listUIPRes, 2, 4, 4);
	}
	
	@Test
	public void testUIPNAConsUsed() 
	{
		List<String> listUIPRes = Arrays.asList();

		runUpdateInPlaceTest(TEST_NAME, 2, listUIPRes, 0, 0, 4);
	}
	
	@Test
	public void testUIPNAConsUsedOutsideDAG()
	{
		List<String> listUIPRes = Arrays.asList();

		runUpdateInPlaceTest(TEST_NAME, 3, listUIPRes, 0, 0, 4);
	}
	
	@Test
	public void testUIPConsNotUsed() 
	{
		List<String> listUIPRes = Arrays.asList("A");

		runUpdateInPlaceTest(TEST_NAME, 4, listUIPRes, 2, 4, 4);
	}
	
	@Test
	public void testUIPNAConsUsedForLoop() 
	{
		List<String> listUIPRes = Arrays.asList();

		runUpdateInPlaceTest(TEST_NAME, 5, listUIPRes, 0, 0, 4);
	}
	
	@Test
	public void testUIPNAComplexConsUsed() 
	{
		List<String> listUIPRes = Arrays.asList();

		runUpdateInPlaceTest(TEST_NAME, 6, listUIPRes, 0, 0, 4);
	}
	
	@Test
	public void testUIPNAComplexCandUsed() 
	{
		List<String> listUIPRes = Arrays.asList();

		runUpdateInPlaceTest(TEST_NAME, 7, listUIPRes, 0, 0, 4);
	}
	
	@Test
	public void testUIPNAConsUsedElse() 
	{
		List<String> listUIPRes = Arrays.asList();

		runUpdateInPlaceTest(TEST_NAME, 8, listUIPRes, 0, 0, 4);
	}
	
	@Test
	public void testUIPNACandInPredicate()
	{
		List<String> listUIPRes = Arrays.asList();

		runUpdateInPlaceTest(TEST_NAME, 9, listUIPRes, 0, 0, 4);
	}
	
	@Test
	public void testUIPNAMultiLIX() 
	{
		List<String> listUIPRes = Arrays.asList();

		runUpdateInPlaceTest(TEST_NAME, 10, listUIPRes, 0, 0, 12);
	}
	
	@Test
	public void testUIPNAParFor() 
	{
		List<String> listUIPRes = Arrays.asList();

		runUpdateInPlaceTest(TEST_NAME, 11, listUIPRes, 0, 0, 8);
	}
	
	@Test
	public void testUIPNACandInFuncCall() 
	{
		List<String> listUIPRes = Arrays.asList();

		runUpdateInPlaceTest(TEST_NAME, 12, listUIPRes, 0, 0, 4);
	}
	
	@Test
	public void testUIPNAConsInFuncCall() 
	{
		List<String> listUIPRes = Arrays.asList();

		runUpdateInPlaceTest(TEST_NAME, 13, listUIPRes, 0, 0, 4);
	}
	
	@Test
	public void testUIPFuncCall() 
	{
		List<String> listUIPRes = Arrays.asList("A");

		runUpdateInPlaceTest(TEST_NAME, 14, listUIPRes, 2, 4, 8);
	}
		
	@Test
	public void testUIPNAConsUsedInsideDAG()
	{
		List<String> listUIPRes = Arrays.asList();

		runUpdateInPlaceTest(TEST_NAME, 15, listUIPRes, 0, 0, 4);
	}
	
	/**
	 * 
	 * @param TEST_NAME
	 * @param iTestNumber
	 * @param listUIPRes
	 */
	private void runUpdateInPlaceTest( String TEST_NAME, int iTestNumber, List<String> listUIPExp, long lTotalUIPVar, long lTotalLixUIP, long lTotalLix)
	{
		boolean oldinplace =  OptimizerUtils.ALLOW_LOOP_UPDATE_IN_PLACE;
		
		try
		{
			TestConfiguration config = getTestConfiguration(TEST_NAME);
			loadTestConfiguration(config);
			
			OptimizerUtils.ALLOW_LOOP_UPDATE_IN_PLACE = false;
					
			// This is for running the junit test the new way, i.e., construct the arguments directly 
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + iTestNumber + ".dml";
			programArgs = new String[]{"-stats"}; //new String[]{"-args", input("A"), output("B") };
			
			runTest(true, false, null, -1);
		}
		finally {
			OptimizerUtils.ALLOW_LOOP_UPDATE_IN_PLACE = oldinplace;
		}
	}
	
}
