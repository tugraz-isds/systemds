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

import org.junit.Test;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;



public class LessThanOrEqualTest extends AutomatedTestBase 
{

	private static final String TEST_DIR = "functions/binary/scalar/";
	private static final String TEST_CLASS_DIR = TEST_DIR + LessThanOrEqualTest.class.getSimpleName() + "/";
	
	@Override
	public void setUp() {
		addTestConfiguration("LessThanOrEqualTest", new TestConfiguration(TEST_CLASS_DIR, "LessThanOrEqualTest",
				new String[] { "left_1", "left_2", "left_3", "right_1", "right_2", "right_3" }));
	}
	
	@Test
	public void testLessThanOrEqual() {
		TestConfiguration config = getTestConfiguration("LessThanOrEqualTest");
		loadTestConfiguration(config);
		
		createHelperMatrix();
		writeExpectedHelperMatrix("left_1", 2);
		writeExpectedHelperMatrix("left_2", 2);
		writeExpectedHelperMatrix("left_3", 1);
		writeExpectedHelperMatrix("right_1", 1);
		writeExpectedHelperMatrix("right_2", 2);
		writeExpectedHelperMatrix("right_3", 2);
		
		runTest();
		
		compareResults();
	}

}
