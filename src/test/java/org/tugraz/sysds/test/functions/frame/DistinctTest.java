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
package org.tugraz.sysds.test.functions.frame;

import org.junit.Test;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.lops.LopProperties;
import org.tugraz.sysds.runtime.io.FrameWriter;
import org.tugraz.sysds.runtime.io.FrameWriterFactory;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.runtime.matrix.data.OutputInfo;
import org.tugraz.sysds.runtime.util.UtilFunctions;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;

import java.io.IOException;

public class DistinctTest extends AutomatedTestBase {
	private final static String TEST_NAME1 = "DistinctTest";
	private final static String TEST_DIR = "functions/frame/";
	private static final String TEST_CLASS_DIR = TEST_DIR + DistinctTest.class.getSimpleName() + "/";

	@Override public void setUp() {
		addTestConfiguration(TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] {"Y"}));
	}

	@Test public void testDistinct1() {
		runTestDistinct(150, LopProperties.ExecType.CP);
	}

	@Test public void testDistinct2() {
		runTestDistinct(200, LopProperties.ExecType.SPARK);
	}

	private void runTestDistinct(int rows, LopProperties.ExecType et) {
		//handle rows and cols
		Types.ExecMode platformOld = rtplatform;
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		if (et == LopProperties.ExecType.SPARK) {
			rtplatform = Types.ExecMode.SPARK;
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;
		}
		else {
			rtplatform = Types.ExecMode.SINGLE_NODE;
		}

		try {
			//register test configuration
			TestConfiguration config = getTestConfiguration(TEST_NAME1);
			loadTestConfiguration(config);
			FrameBlock frame1 = new FrameBlock(new Types.ValueType[] {Types.ValueType.STRING});
			FrameWriter writer = FrameWriterFactory.createFrameWriter(OutputInfo.CSVOutputInfo);
			/* This is for running the junit test the new way, i.e., construct the arguments directly */
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME1 + ".dml";
			programArgs = new String[] {"-args", input("X"), String.valueOf(rows), String.valueOf(1), output("B")};

			double[][] X =  getRandomMatrix(rows, 1, 1, 10, 1, 10);
			initFrameDataDouble(frame1, X, new Types.ValueType[] {Types.ValueType.STRING}, rows);
			writer.writeFrameToHDFS(frame1, input("X"), rows, 1);

			runTest(true, false, null, -1);

		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
			rtplatform = platformOld;
		}
	}
	private static void initFrameDataDouble(FrameBlock frame, double[][] data, Types.ValueType[] lschema, int rows) {
		Object[] row1 = new Object[lschema.length];
		for (int i = 0; i < rows; i++) {
			data[i][0] = UtilFunctions.objectToDouble(Types.ValueType.STRING,
					row1[0] = UtilFunctions.doubleToObject(Types.ValueType.STRING, data[i][0]));
			frame.appendRow(row1);
		}
	}
}
