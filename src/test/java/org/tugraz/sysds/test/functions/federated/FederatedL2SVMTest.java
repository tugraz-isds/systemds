/*
 * Copyright 2019 Graz University of Technology
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
 *
 */

package org.tugraz.sysds.test.functions.federated;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.runtime.meta.MatrixCharacteristics;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

import java.util.Arrays;
import java.util.Collection;

import static java.lang.Thread.sleep;

@RunWith(value = Parameterized.class)
public class FederatedL2SVMTest extends AutomatedTestBase {
	
	private final static String TEST_DIR = "functions/federated/";
	private final static String TEST_NAME = "FederatedL2SVMTest";
	private final static String TEST_CLASS_DIR = TEST_DIR + FederatedL2SVMTest.class.getSimpleName() + "/";
	
	private final static int blocksize = 1024;
	private final static int port1 = 1222;
	private final static int port2 = 1223;
	private int rows, cols;
	
	public FederatedL2SVMTest(int rows, int cols) {
		this.rows = rows;
		this.cols = cols;
	}
	
	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] {"Z"}));
	}
	
	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		// rows have to be even and > 1
		Object[][] data = new Object[][] {{2, 1000}, {10, 100}, {100, 10}, {1000, 1}, {10, 2000}, {2000, 10}};
		return Arrays.asList(data);
	}
	
	@Test
	public void federatedL2SVMCP() {
		federatedL2SVM(Types.ExecMode.SINGLE_NODE);
	}
	
	/*TODO support SPARK execution mode -> RDDs and SPARK instructions lead to quite a few problems
	@Test
	public void federatedL2SVMSP() {
		federatedL2SVM(Types.ExecMode.SPARK);
	}*/
	
	public void federatedL2SVM(Types.ExecMode execMode) {
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		Types.ExecMode platformOld = rtplatform;
		rtplatform = execMode;
		if (rtplatform == Types.ExecMode.SPARK) {
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;
		}
		Thread t1 = null, t2 = null;
		try {
			getAndLoadTestConfiguration(TEST_NAME);
			String HOME = SCRIPT_DIR + TEST_DIR;
			
			// write input matrices
			int halfRows = rows / 2;
			// We have two matrices handled by a single federated worker
			double[][] X1 = getRandomMatrix(halfRows, cols, 0, 1, 1, 42);
			double[][] X2 = getRandomMatrix(halfRows, cols, 0, 1, 1, 1340);
			double[][] Y = getRandomMatrix(rows, 1, -1, 1, 1, 1233);
			for (int i = 0; i < rows; i++)
				Y[i][0] = (Y[i][0] > 0) ? 1 : -1;
			
			writeInputMatrixWithMTD("X1", X1, false,
				new MatrixCharacteristics(halfRows, cols, blocksize, halfRows * cols));
			writeInputMatrixWithMTD("X2", X2, false,
				new MatrixCharacteristics(halfRows, cols, blocksize, halfRows * cols));
			writeInputMatrixWithMTD("Y", Y, false, new MatrixCharacteristics(rows, 1, blocksize, rows));
			
			// empty script name because we don't execute any script, just start the worker
			fullDMLScriptName = "";
			programArgs = new String[] {"-w", Integer.toString(port1)};
			t1 = new Thread(() -> runTest(true, false, null, -1));
			t1.start();
			sleep(FED_WORKER_WAIT);
			fullDMLScriptName = "";
			programArgs = new String[] {"-w", Integer.toString(port2)};
			t2 = new Thread(() -> runTest(true, false, null, -1));
			t2.start();
			sleep(FED_WORKER_WAIT);
			
			TestConfiguration config = availableTestConfigurations.get(TEST_NAME);
			loadTestConfiguration(config);
			
			// Run reference dml script with normal matrix
			fullDMLScriptName = HOME + TEST_NAME + "Reference.dml";
			programArgs = new String[] {"-args", input("X1"), input("X2"), input("Y"), expected("Z")};
			runTest(true, false, null, -1);
			
			// Run actual dml script with federated matrix
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[] {"-explain", "-args", "\"localhost:" + port1 + "/" + input("X1") + "\"",
					"\"localhost:" + port2 + "/" + input("X2") + "\"", Integer.toString(rows), Integer.toString(cols),
					Integer.toString(halfRows), input("Y"), output("Z")};
			runTest(true, false, null, -1);
			
			// compare via files
			compareResults(1e-9);
		}
		catch (InterruptedException e) {
			e.printStackTrace();
			assert (false);
		}
		finally {
			TestUtils.shutdownThreads(t1, t2);
			rtplatform = platformOld;
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
		}
	}
}
