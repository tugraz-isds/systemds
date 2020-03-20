/*
 * Copyright 2020 Graz University of Technology
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tugraz.sysds.test.functions.builtin;

import org.junit.Assert;
import org.junit.Test;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.lops.LopProperties;
import org.tugraz.sysds.runtime.io.FrameWriter;
import org.tugraz.sysds.runtime.io.FrameWriterFactory;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.runtime.matrix.data.InputInfo;
import org.tugraz.sysds.runtime.matrix.data.OutputInfo;
import org.tugraz.sysds.runtime.util.UtilFunctions;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

import java.io.IOException;

public class BuiltinImputeRFDTest extends AutomatedTestBase {

	private final static String TEST_NAME = "impute_RFD";
	private final static String TEST_DIR = "functions/builtin/";
	private static final String TEST_CLASS_DIR = TEST_DIR + BuiltinImputeRFDTest.class.getSimpleName() + "/";
	private final static int rows = 10;
	private final static int cols = 4;
	private final static double epsilon = 0.0000000001;

	private final static Types.ValueType[] schema = {Types.ValueType.BOOLEAN, Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.FP64};

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] {"C"}));
	}

	@Test
	public void test1() throws IOException {
		double[][] M = {{0, 1, 1, 0}};
		runImpute_RFDTests(M, 0.75, 1,  LopProperties.ExecType.CP);
	}

	@Test
	public void test2() throws IOException {
		double[][] M = {{0, 1, 1, 0}};
		runImpute_RFDTests(M, 0.75, 2,  LopProperties.ExecType.CP);
	}
//
//	@Test
//	public void test3() throws IOException {
//		double[][] M = {{0, 1, 1, 0}};
//		runImpute_RFDTests(M, 0.75, 1,  LopProperties.ExecType.SPARK);
//	}
//
//	@Test
//	public void test4() throws IOException {
//		double[][] M = {{0, 1, 1, 0}};
//		runImpute_RFDTests(M, 0.75, 2,  LopProperties.ExecType.SPARK);
//	}
	private void runImpute_RFDTests(double[][] M, double threshold, int test, LopProperties.ExecType instType)
			throws IOException {
		Types.ExecMode platformOld = setExecMode(instType);
		try {
			loadTestConfiguration(getTestConfiguration(TEST_NAME));
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[] {"-args", input("A"), input("M"), String.valueOf(threshold), output("B")}; //
			//initialize the frame data.
			FrameBlock frame1 = new FrameBlock(schema);
			FrameWriter writer = FrameWriterFactory.createFrameWriter(OutputInfo.CSVOutputInfo);
			double[][] A = getRandomMatrix(rows, cols, 0, 1, 0.7, -1);
			initFrameDataString(frame1, A, test);
			writer.writeFrameToHDFS(frame1.slice(0, rows - 1, 0, schema.length - 1, new FrameBlock()),
					input("A"), rows, schema.length);
			writeInputMatrixWithMTD("M", M, true);

			runTest(true, false, null, -1);
			FrameBlock frameRead = readDMLFrameFromHDFS("B", InputInfo.BinaryBlockInputInfo);
			FrameBlock realFrame = tureOutput(A);
			verifyFrameData(frameRead, realFrame, schema);
		}
		finally {
			rtplatform = platformOld;
		}
	}

	private static void initFrameDataString(FrameBlock frame1, double[][] data, int test) {
		boolean[] b = new boolean[rows];
		long[] l = new long[rows];
		String[] s1 = null, s2 = null;
		for (int i = 0; i < rows; i++) {
			data[i][1] = (b[i] = (Boolean) UtilFunctions.doubleToObject(Types.ValueType.BOOLEAN, data[i][1], false)) ? 1 : 0;
			l[i] = (Long) UtilFunctions.doubleToObject(Types.ValueType.INT64, data[i][2], false);
		}
		switch (test)
		{
			case 1:
				s1 = new String[] {"TU-Graz", "TU-Graz", "TU-Graz", "IIT", "IIT", "IIT", "SIBA", "SIBA", "SIBA", "TU-Wien"};
				s2 = new String[] {"Austria", "Austria", "Austria", "India", null, "India", "Pakistan", "Pakistan", "Austria", "Austria"};
				break;
			case 2:
				s1 = new String[]  {"TU-Graz", "TU-Graz", "TU-Graz", "IIT", "IIT", "IIT", "SIBA", "SIBA", "SIBA", "TU-Wien"};
				s2 = new String[] {"Austria", "Austria", "Austria", null, null, "India", "Pakistan", "Pakistan", null,"Austria"};
				break;  //{"TU Graz", "TU Graz", "TU Graz", "IIT", "IIT", "IIT", "SIBA", "SIBA", "SIBA", "TU Wien"};
		}

		frame1.appendColumn(b);
		frame1.appendColumn(s1);
		frame1.appendColumn(s2);
		frame1.appendColumn(l);
	}

	private static FrameBlock tureOutput(double[][] data) {
		FrameBlock frame1 = new FrameBlock(schema);
		boolean[] b = new boolean[rows];
		String[] s1 = {"TU-Graz", "TU-Graz", "TU-Graz", "IIT", "IIT", "IIT", "SIBA", "SIBA", "SIBA", "TU-Wien"};
		String[] s2 = {"Austria", "Austria", "Austria", "India", "India", "India", "Pakistan", "Pakistan", "Pakistan", "Austria"};
		long[] l = new long[rows];
		for (int i = 0; i < rows; i++) {
			data[i][1] = (b[i] = (Boolean) UtilFunctions.doubleToObject(Types.ValueType.BOOLEAN, data[i][1], false)) ? 1 : 0;
			l[i] = (Long) UtilFunctions.doubleToObject(Types.ValueType.INT64, data[i][2], false);
		}
		frame1.appendColumn(b);
		frame1.appendColumn(s1);
		frame1.appendColumn(s2);
		frame1.appendColumn(l);
		return frame1;
	}

	private static void verifyFrameData(FrameBlock frame1, FrameBlock frame2, Types.ValueType[] schema) {
		for (int i = 0; i < frame1.getNumRows(); i++)
			for (int j = 0; j < frame1.getNumColumns(); j++) {
				Object val1 = UtilFunctions.stringToObject(schema[j], UtilFunctions.objectToString(frame1.get(i, j)));
				Object val2 = UtilFunctions.stringToObject(schema[j], UtilFunctions.objectToString(frame2.get(i, j)));
				if (TestUtils.compareToR(schema[j], val1, val2, epsilon) != 0)
					Assert.fail("The DML data for cell (" + i + "," + j + ") is " + val1 + ", not same as the R value " + val2);
			}
	}
}