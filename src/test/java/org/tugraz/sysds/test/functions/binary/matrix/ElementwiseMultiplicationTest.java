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

package org.tugraz.sysds.test.functions.binary.matrix;

import org.junit.Test;
import org.tugraz.sysds.api.DMLException;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;


public class ElementwiseMultiplicationTest extends AutomatedTestBase 
{
	
	private static final String TEST_DIR = "functions/binary/matrix/";
	private static final String TEST_CLASS_DIR = TEST_DIR + ElementwiseMultiplicationTest.class.getSimpleName() + "/";
	
	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();

		// positive tests
		addTestConfiguration("DenseTest", new TestConfiguration(TEST_CLASS_DIR,"ElementwiseMultiplicationTest",
				new String[] { "c" }));
		addTestConfiguration("SparseTest", new TestConfiguration(TEST_CLASS_DIR,"ElementwiseMultiplicationTest",
				new String[] { "c" }));
		addTestConfiguration("EmptyTest", new TestConfiguration(TEST_CLASS_DIR,"ElementwiseMultiplicationTest",
				new String[] { "c" }));
		addTestConfiguration("WrongDimensionLessRowsTest", new TestConfiguration(TEST_CLASS_DIR,
				"ElementwiseMultiplicationVariableDimensionsTest", new String[] { "c" }));
		addTestConfiguration("WrongDimensionMoreRowsTest", new TestConfiguration(TEST_CLASS_DIR,
				"ElementwiseMultiplicationVariableDimensionsTest", new String[] { "c" }));
		addTestConfiguration("WrongDimensionLessColsTest", new TestConfiguration(TEST_CLASS_DIR,
				"ElementwiseMultiplicationVariableDimensionsTest", new String[] { "c" }));
		addTestConfiguration("WrongDimensionMoreColsTest", new TestConfiguration(TEST_CLASS_DIR,
				"ElementwiseMultiplicationVariableDimensionsTest", new String[] { "c" }));
		addTestConfiguration("WrongDimensionLessRowsLessColsTest", new TestConfiguration(TEST_CLASS_DIR,
				"ElementwiseMultiplicationVariableDimensionsTest", new String[] { "c" }));
		addTestConfiguration("WrongDimensionMoreRowsMoreColsTest", new TestConfiguration(TEST_CLASS_DIR,
				"ElementwiseMultiplicationVariableDimensionsTest", new String[] { "c" }));
		addTestConfiguration("WrongDimensionLessRowsMoreColsTest", new TestConfiguration(TEST_CLASS_DIR,
				"ElementwiseMultiplicationVariableDimensionsTest", new String[] { "c" }));
		addTestConfiguration("WrongDimensionMoreRowsLessColsTest", new TestConfiguration(TEST_CLASS_DIR,
				"ElementwiseMultiplicationVariableDimensionsTest", new String[] { "c" }));

		// negative tests
	}

	@Test
	public void testDense() {
		int rows = 10;
		int cols = 10;

		TestConfiguration config = getTestConfiguration("DenseTest");
		config.addVariable("rows", rows);
		config.addVariable("cols", cols);

		loadTestConfiguration(config);

		double[][] a = getRandomMatrix(rows, cols, -1, 1, 1, -1);
		double[][] b = getRandomMatrix(rows, cols, -1, 1, 1, -1);
		double[][] c = new double[rows][cols];
		for (int i = 0; i < rows; i++) {
			for (int j = 0; j < cols; j++) {
				c[i][j] = a[i][j] * b[i][j];
			}
		}

		writeInputMatrix("a", a);
		writeInputMatrix("b", b);
		writeExpectedMatrix("c", c);

		runTest();

		compareResults();
	}

	@Test
	public void testSparse() {
		int rows = 50;
		int cols = 50;

		TestConfiguration config = getTestConfiguration("SparseTest");
		config.addVariable("rows", rows);
		config.addVariable("cols", cols);

		loadTestConfiguration(config);

		double[][] a = getRandomMatrix(rows, cols, -1, 1, 0.05, -1);
		double[][] b = getRandomMatrix(rows, cols, -1, 1, 0.05, -1);
		double[][] c = new double[rows][cols];
		for (int i = 0; i < rows; i++) {
			for (int j = 0; j < cols; j++) {
				c[i][j] = a[i][j] * b[i][j];
			}
		}

		writeInputMatrix("a", a);
		writeInputMatrix("b", b);
		writeExpectedMatrix("c", c);

		runTest();

		compareResults();
	}

	@Test
	public void testWrongDimensionsLessRows() {
		int rows1 = 8;
		int cols1 = 10;
		int rows2 = 10;
		int cols2 = 10;

		TestConfiguration config = getTestConfiguration("WrongDimensionLessRowsTest");
		config.addVariable("rows1", rows1);
		config.addVariable("cols1", cols1);
		config.addVariable("rows2", rows2);
		config.addVariable("cols2", cols2);

		loadTestConfiguration(config);

		runTest(true, DMLException.class);
	}

	@Test
	public void testWrongDimensionsMoreRows() {
		int rows1 = 12;
		int cols1 = 10;
		int rows2 = 10;
		int cols2 = 10;

		TestConfiguration config = getTestConfiguration("WrongDimensionMoreRowsTest");
		config.addVariable("rows1", rows1);
		config.addVariable("cols1", cols1);
		config.addVariable("rows2", rows2);
		config.addVariable("cols2", cols2);

		loadTestConfiguration(config);

		runTest(true, DMLException.class);
	}

	@Test
	public void testWrongDimensionsLessCols() {
		int rows1 = 10;
		int cols1 = 8;
		int rows2 = 10;
		int cols2 = 10;

		TestConfiguration config = getTestConfiguration("WrongDimensionLessColsTest");
		config.addVariable("rows1", rows1);
		config.addVariable("cols1", cols1);
		config.addVariable("rows2", rows2);
		config.addVariable("cols2", cols2);

		loadTestConfiguration(config);

		runTest(true, DMLException.class);
	}

	@Test
	public void testWrongDimensionsMoreCols() {
		int rows1 = 10;
		int cols1 = 12;
		int rows2 = 10;
		int cols2 = 10;

		TestConfiguration config = getTestConfiguration("WrongDimensionMoreColsTest");
		config.addVariable("rows1", rows1);
		config.addVariable("cols1", cols1);
		config.addVariable("rows2", rows2);
		config.addVariable("cols2", cols2);

		loadTestConfiguration(config);

		runTest(true, DMLException.class);
	}

	@Test
	public void testWrongDimensionsLessRowsLessCols() {
		int rows1 = 8;
		int cols1 = 8;
		int rows2 = 10;
		int cols2 = 10;

		TestConfiguration config = getTestConfiguration("WrongDimensionLessRowsLessColsTest");
		config.addVariable("rows1", rows1);
		config.addVariable("cols1", cols1);
		config.addVariable("rows2", rows2);
		config.addVariable("cols2", cols2);

		loadTestConfiguration(config);

		runTest(true, DMLException.class);
	}

	@Test
	public void testWrongDimensionsMoreRowsMoreCols() {
		int rows1 = 12;
		int cols1 = 12;
		int rows2 = 10;
		int cols2 = 10;

		TestConfiguration config = getTestConfiguration("WrongDimensionMoreRowsMoreColsTest");
		config.addVariable("rows1", rows1);
		config.addVariable("cols1", cols1);
		config.addVariable("rows2", rows2);
		config.addVariable("cols2", cols2);

		loadTestConfiguration(config);

		runTest(true, DMLException.class);
	}

	@Test
	public void testWrongDimensionsLessRowsMoreCols() {
		int rows1 = 8;
		int cols1 = 12;
		int rows2 = 10;
		int cols2 = 10;

		TestConfiguration config = getTestConfiguration("WrongDimensionLessRowsMoreColsTest");
		config.addVariable("rows1", rows1);
		config.addVariable("cols1", cols1);
		config.addVariable("rows2", rows2);
		config.addVariable("cols2", cols2);

		loadTestConfiguration(config);

		runTest(true, DMLException.class);
	}

	@Test
	public void testWrongDimensionsMoreRowsLessCols() {
		int rows1 = 12;
		int cols1 = 8;
		int rows2 = 10;
		int cols2 = 10;

		TestConfiguration config = getTestConfiguration("WrongDimensionMoreRowsLessColsTest");
		config.addVariable("rows1", rows1);
		config.addVariable("cols1", cols1);
		config.addVariable("rows2", rows2);
		config.addVariable("cols2", cols2);

		loadTestConfiguration(config);

		runTest(true, DMLException.class);
	}
}
