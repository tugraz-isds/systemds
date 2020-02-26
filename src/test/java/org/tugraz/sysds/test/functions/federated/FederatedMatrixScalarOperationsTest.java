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

package org.tugraz.sysds.test.functions.federated;


import org.junit.Test;
import org.junit.runners.Parameterized;
import org.junit.runner.RunWith;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import java.util.Arrays;


@RunWith(Parameterized.class)
public class FederatedMatrixScalarOperationsTest extends AutomatedTestBase
{
	@Parameterized.Parameters
	public static Iterable<Object[]> data() {
		return Arrays.asList(new Object[][] {
			{ 1, 1 }
//			{ 1, 100 },
//			{ 1, 10000 },
//			{ 100, 1 },
//			{ 100, 100 },
//			{ 100, 10000 },
//			{ 10000, 1 },
//			{ 10000, 100 },
//			{ 10000, 10000 }
		 });
	}

	//internals 4 parameterized tests
	@Parameterized.Parameter(0)
	public Integer rows;
	@Parameterized.Parameter(1)
	public Integer cols;

	//System test paths
	private static final String TEST_DIR = "functions/federated/matrix_scalar/";
	private static final String TEST_CLASS_DIR = TEST_DIR + FederatedMatrixScalarOperationsTest.class.getSimpleName() + "/";
	private static final String MATRIX_TEST_FILE = "M";
	
	@Override
	public void setUp() 
	{

		//int matrix_rows = 

		//Create test matrix used in all cases
		//currently federated Read only supports MTD files
		/* writeInputMatrixWithMTD(
			MATRIX_TEST_FILE,
			getRandomMatrix(rows, cols, -10, 10, 1, 1)
			, false, new MatrixCharacteristics(rows, cols, blocksize, rows * cols)); */


		addTestConfiguration("FederatedMatrixLocalScalarAdditionTest", 
			new TestConfiguration(TEST_CLASS_DIR, "FederatedMatrixLocalScalarAdditionTest", new String [] {"result"}));
    }
    
    @Test
	public void testLocalIntegerFederatedMatrixAddition() {
		TestConfiguration config = availableTestConfigurations.get("FederatedMatrixLocalScalarAdditionTest");
/* 		config.addVariable("rows", rows);
		config.addVariable("cols", cols);
		config.addVariable("summand", 2); */

		loadTestConfiguration(config);
		runTest();
	}
}