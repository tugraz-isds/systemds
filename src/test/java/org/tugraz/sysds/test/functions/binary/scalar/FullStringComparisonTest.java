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

package org.tugraz.sysds.test.functions.binary.scalar;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.tugraz.sysds.runtime.util.HDFSTool;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;

/**
 * The main purpose of this test is to verify all combinations of
 * scalar string comparisons.
 * 
 */
public class FullStringComparisonTest extends AutomatedTestBase 
{
	
	private final static String TEST_NAME1 = "FullStringComparisonTest";
	private final static String TEST_DIR = "functions/binary/scalar/";
	private static final String TEST_CLASS_DIR = TEST_DIR + FullStringComparisonTest.class.getSimpleName() + "/";
	
	public enum Type{
		GREATER,
		LESS,
		EQUALS,
		NOT_EQUALS,
		GREATER_EQUALS,
		LESS_EQUALS,
	}
	
	
	@Override
	public void setUp() 
	{
		addTestConfiguration( TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] { "B" })   ); 
	}

	@Test
	public void testStringCompareEqualsTrue() 
	{
		runStringComparison(Type.EQUALS, true);
	}
	
	@Test
	public void testStringCompareEqualsFalse() 
	{
		runStringComparison(Type.EQUALS, false);
	}
	
	@Test
	public void testStringCompareNotEqualsTrue() 
	{
		runStringComparison(Type.NOT_EQUALS, true);
	}
	
	@Test
	public void testStringCompareNotEqualsFalse() 
	{
		runStringComparison(Type.NOT_EQUALS, false);
	}
	
	@Test
	public void testStringCompareGreaterTrue() 
	{
		runStringComparison(Type.GREATER, true);
	}
	
	@Test
	public void testStringCompareGreaterFalse() 
	{
		runStringComparison(Type.GREATER, false);
	}
	
	@Test
	public void testStringCompareGreaterEqualsTrue() 
	{
		runStringComparison(Type.GREATER_EQUALS, true);
	}
	
	@Test
	public void testStringCompareGreaterEqualsFalse() 
	{
		runStringComparison(Type.GREATER_EQUALS, false);
	}
	
	@Test
	public void testStringCompareLessTrue() 
	{
		runStringComparison(Type.LESS, true);
	}
	
	@Test
	public void testStringCompareLessFalse() 
	{
		runStringComparison(Type.LESS, false);
	}
	
	@Test
	public void testStringCompareLessEqualsTrue() 
	{
		runStringComparison(Type.LESS_EQUALS, true);
	}
	
	@Test
	public void testStringCompareLessEqualsFalse() 
	{
		runStringComparison(Type.LESS_EQUALS, false);
	}
	
	
	/**
	 * 
	 * @param type
	 * @param instType
	 * @param sparse
	 */
	private void runStringComparison( Type type, boolean trueCondition )
	{
		String TEST_NAME = TEST_NAME1;
		
		String string1 = "abcd";
		String string2 = null;
		switch( type ){
			case EQUALS:     string2 = trueCondition ? "abcd" : "xyz"; break;
			case NOT_EQUALS: string2 = trueCondition ? "xyz" : "abcd"; break;
			case LESS:       string2 = trueCondition ? "dcba" : "aabbccdd"; break;
			case LESS_EQUALS: string2 = trueCondition ? "abce" : "aabbccdd"; break;
			case GREATER:     string2 = trueCondition ? "aabbccdd" : "dcba"; break;
			case GREATER_EQUALS: string2 = trueCondition ? "aabbccdd" : "abce"; break;
		}
		
		getAndLoadTestConfiguration(TEST_NAME);
			
		String HOME = SCRIPT_DIR + TEST_DIR;
		fullDMLScriptName = HOME + TEST_NAME + ".dml";
		programArgs = new String[]{"-args",
			string1, string2, Integer.toString(type.ordinal()), output("B") };

		//run tests
		runTest(true, false, null, -1); 
		
		//compare result
		try {
			boolean retCondition = HDFSTool.readBooleanFromHDFSFile(output("B"));
			Assert.assertEquals(trueCondition, retCondition);
		} 
		catch (IOException e) {
			Assert.fail(e.getMessage());
		}
	}
}