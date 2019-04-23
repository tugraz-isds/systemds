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

package org.tugraz.sysds.test.functions.recompile;

import java.util.HashMap;

import org.junit.Test;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.io.FileFormatPropertiesCSV;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.data.OutputInfo;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.runtime.meta.MatrixCharacteristics;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.runtime.util.HDFSTool;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class CSVReadUnknownSizeTest extends AutomatedTestBase {

	private final static String TEST_NAME = "csv_read_unknown";
	private final static String TEST_DIR = "functions/recompile/";
	private final static String TEST_CLASS_DIR = TEST_DIR + CSVReadUnknownSizeTest.class.getSimpleName() + "/";

	private final static int rows = 10;
	private final static int cols = 15;

	/** Main method for running one test at a time from Eclipse. */
	public static void main(String[] args) {
		long startMsec = System.currentTimeMillis();

		CSVReadUnknownSizeTest t = new CSVReadUnknownSizeTest();
		t.setUpBase();
		t.setUp();
		t.testCSVReadUnknownSizeSplitRewrites();
		t.tearDown();

		long elapsedMsec = System.currentTimeMillis() - startMsec;
		System.err.printf("Finished in %1.3f sec\n", elapsedMsec / 1000.0);
	}

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, 
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] { "X" }));
	}

	@Test
	public void testCSVReadUnknownSizeNoSplitNoRewrites() {
		runCSVReadUnknownSizeTest(false, false);
	}

	@Test
	public void testCSVReadUnknownSizeNoSplitRewrites() {
		runCSVReadUnknownSizeTest(false, true);
	}

	@Test
	public void testCSVReadUnknownSizeSplitNoRewrites() {
		runCSVReadUnknownSizeTest(true, false);
	}

	@Test
	public void testCSVReadUnknownSizeSplitRewrites() {
		runCSVReadUnknownSizeTest(true, true);
	}

	/**
	 * 
	 * @param condition
	 * @param branchRemoval
	 * @param IPA
	 */
	private void runCSVReadUnknownSizeTest( boolean splitDags, boolean rewrites )
	{	
		boolean oldFlagSplit = OptimizerUtils.ALLOW_SPLIT_HOP_DAGS;
		boolean oldFlagRewrites = OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION;
		
		try
		{
			getAndLoadTestConfiguration(TEST_NAME);
			
			/* This is for running the junit test the new way, i.e., construct the arguments directly */
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-explain", "-args", input("X"), output("R") };
			
			fullRScriptName = HOME + TEST_NAME + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " + expectedDir();

			OptimizerUtils.ALLOW_SPLIT_HOP_DAGS = splitDags;
			OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = rewrites;

			double[][] X = getRandomMatrix(rows, cols, -1, 1, 1.0d, 7);
			MatrixBlock mb = DataConverter.convertToMatrixBlock(X);
			MatrixCharacteristics mc = new MatrixCharacteristics(rows, cols, 1000, 1000);
			FileFormatPropertiesCSV fprop = new FileFormatPropertiesCSV();			
			DataConverter.writeMatrixToHDFS(mb, input("X"), OutputInfo.CSVOutputInfo, mc, -1, fprop);
			mc.set(-1, -1, -1, -1);
			HDFSTool.writeMetaDataFile(input("X.mtd"), ValueType.FP64, mc, OutputInfo.CSVOutputInfo, fprop);
			
			runTest(true, false, null, -1); 
			
			//compare matrices 
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("R");
			for( int i=0; i<rows; i++ )
				for( int j=0; j<cols; j++ )
				{
					Double tmp = dmlfile.get(new CellIndex(i+1,j+1));
					
					double expectedValue = mb.quickGetValue(i, j);
					double actualValue =  (tmp==null)?0.0:tmp;
					
					if (expectedValue != actualValue) {
						throw new Exception(String.format("Value of cell (%d,%d) "
								+ "(zero-based indices) in output file %s is %f, "
								+ "but original value was %f",
								i, j, baseDirectory + OUTPUT_DIR + "R",
								actualValue, expectedValue));
					}
				}
			
			
			//check expected number of compiled and executed MR jobs
			//note: with algebraic rewrites - unary op in reducer prevents job-level recompile
			//TODO investigate current number of spark instructions
			int expectedNumCompiled = (rewrites && !splitDags) ? 5 : 5; //reblock, GMR
			int expectedNumExecuted = splitDags ? 2 : rewrites ? 5 : 5;
			
			checkNumCompiledSparkInst(expectedNumCompiled);
			checkNumExecutedSparkInst(expectedNumExecuted);
		}
		catch(Exception ex)
		{
			throw new RuntimeException(ex);
		}
		finally
		{
			OptimizerUtils.ALLOW_SPLIT_HOP_DAGS = oldFlagSplit;
			OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = oldFlagRewrites;
		}
	}
}