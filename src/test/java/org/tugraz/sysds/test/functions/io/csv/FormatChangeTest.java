/*
 * Modifications Copyright 2019 Graz University of Technology
 *
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

package org.tugraz.sysds.test.functions.io.csv;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.runtime.meta.MatrixCharacteristics;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

@RunWith(value = Parameterized.class)
@net.jcip.annotations.NotThreadSafe
public class FormatChangeTest extends AutomatedTestBase 
{
	
	private final static String TEST_NAME = "csv_test";
	private final static String TEST_DIR = "functions/io/csv/";
	private final static String TEST_CLASS_DIR = TEST_DIR + FormatChangeTest.class.getSimpleName() + "/";
	
	//private final static int rows = 1200;
	//private final static int cols = 100;
	//private final static double sparsity = 1;
	private static String format1, format2;
	private final static double eps = 1e-9;

	private int _rows, _cols;
	private double _sparsity;
	
	/** Main method for running one test at a time from Eclipse. */
	public static void main(String[] args) {
		long startMsec = System.currentTimeMillis();
		
		// Test has multiple parametrized runs.  Pick just one.
		List<Object[]> data = (List<Object[]>) data();
		Object[] chosenData = data.get(0);
		
		FormatChangeTest t= new FormatChangeTest((Integer)chosenData[0],
				(Integer)chosenData[1], (Double)chosenData[2]);
		t.setUpBase();
		t.setUp();
		t.testFormatChangeCP();
		t.tearDown();
		
		long elapsedMsec = System.currentTimeMillis() - startMsec;
		System.err.printf("Finished in %1.3f sec\n", elapsedMsec / 1000.0);
	}
	
	@Override
	public void setUp() 
	{
		addTestConfiguration(TEST_NAME, 
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] { "Rout" }) );
	}
	
	public FormatChangeTest(int r, int c, double sp) {
		_rows = r; 
		_cols = c; 
		_sparsity = sp;
	}

	@Parameters
	public static Collection<Object[]> data() {
		Object[][] data = new Object[][] { { 2000, 500, 0.01 }, { 1500, 150, 1 } };
		return Arrays.asList(data);
	}
	
	private void setup() {
		
		TestConfiguration config = getTestConfiguration(TEST_NAME);
		config.addVariable("rows", _rows);
		config.addVariable("cols", _cols);
		config.addVariable("format1", "text");
		config.addVariable("format2", "binary");
		
		loadTestConfiguration(config);
	}
	
	@Test
	public void testFormatChangeCP() {
		setup();
		ExecMode old_platform = rtplatform;
		rtplatform = ExecMode.SINGLE_NODE;
		formatChangeTest();
		rtplatform =  old_platform;
	}
	
	@Test
	public void testFormatChangeHybrid() {
		setup();
		ExecMode old_platform = rtplatform;
		rtplatform = ExecMode.HYBRID;
		formatChangeTest();
		rtplatform =  old_platform;
	}
	
	private void formatChangeTest() {

		int rows = _rows;
		int cols = _cols;
		double sparsity = _sparsity;

		//generate actual dataset
		double[][] D = getRandomMatrix(rows, cols, 0, 1, sparsity, 7777); 
		MatrixCharacteristics mc = new MatrixCharacteristics(rows, cols, -1, -1);
		writeInputMatrixWithMTD("D", D, true, mc);

		/* This is for running the junit test the new way, i.e., construct the arguments directly */
		String HOME = SCRIPT_DIR + TEST_DIR;
		fullDMLScriptName = HOME + TEST_NAME + ".dml";
		String[] oldProgramArgs = programArgs = new String[]{"-args", 
			input("D"), format1, input("D.binary"), format2 };
		
		String txtFile = input("D");
		String binFile = input("D.binary");
		String csvFile  = output("D.csv");
		
		// text to binary format
		programArgs[2] = "text";
		programArgs[3] = binFile;
		programArgs[4] = "binary";
		runTest(true, false, null, -1);

		// Test TextCell -> CSV conversion
		System.out.println("TextCell -> CSV");
		programArgs[2] = "text";
		programArgs[3] = csvFile;
		programArgs[4] = "csv";
		runTest(true, false, null, -1);
		
		compareFiles(rows, cols, sparsity, txtFile, "text", csvFile);

		// Test BinaryBlock -> CSV conversion
		System.out.println("BinaryBlock -> CSV");
		programArgs = oldProgramArgs;
		programArgs[1] = binFile;
		programArgs[2] = "binary";
		programArgs[3] = csvFile;
		programArgs[4] = "csv";
		runTest(true, false, null, -1);
		
		compareFiles(rows, cols, sparsity, binFile, "binary", csvFile);

		// Test CSV -> TextCell conversion
		System.out.println("CSV -> TextCell");
		programArgs = oldProgramArgs;
		programArgs[1] = csvFile;
		programArgs[2] = "csv";
		programArgs[3] = txtFile;
		programArgs[4] = "text";
		runTest(true, false, null, -1);
		
		compareFiles(rows, cols, sparsity, txtFile, "text", csvFile);

		// Test CSV -> BinaryBlock conversion
		System.out.println("CSV -> BinaryBlock");
		programArgs = oldProgramArgs;
		programArgs[1] = csvFile;
		programArgs[2] = "csv";
		programArgs[3] = binFile;
		programArgs[4] = "binary";
		runTest(true, false, null, -1);
		
		compareFiles(rows, cols, sparsity, binFile, "binary", csvFile);

		//fullRScriptName = HOME + TEST_NAME + ".R";
		//rCmd = "Rscript" + " " + fullRScriptName + " " + 
		//      HOME + INPUT_DIR + " " + Integer.toString((int)maxVal) + " " + HOME + EXPECTED_DIR;

	}
	
	private void compareFiles(int rows, int cols, double sparsity, String dmlFile, String dmlFormat, String csvFile) {
		String HOME = SCRIPT_DIR + TEST_DIR;
		
		// backup old DML and R script files
		String oldDMLScript = fullDMLScriptName;
		String oldRScript = fullRScriptName;
		
		String dmlOutput = output("dml.scalar");
		String rOutput = output("R.scalar");
		
		fullDMLScriptName = HOME + "csv_verify.dml";
		programArgs = new String[]{"-args", dmlFile,
			Integer.toString(rows), Integer.toString(cols), dmlFormat, dmlOutput };
		
		// Check if input csvFile is a directory
		try {
			csvFile = TestUtils.processMultiPartCSVForR(csvFile);
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
		
		fullRScriptName = HOME + "csv_verify.R";
		rCmd = "Rscript" + " " + fullRScriptName + " " + csvFile + " " + rOutput;
		
		// Run the verify test
		runTest(true, false, null, -1);	
		runRScript(true);
		
		double dmlScalar = TestUtils.readDMLScalar(dmlOutput); 
		double rScalar = TestUtils.readRScalar(rOutput); 
		
		TestUtils.compareScalars(dmlScalar, rScalar, eps);
		
		// restore old DML and R script files
		fullDMLScriptName = oldDMLScript;
		fullRScriptName = oldRScript;
		
	}
	
}