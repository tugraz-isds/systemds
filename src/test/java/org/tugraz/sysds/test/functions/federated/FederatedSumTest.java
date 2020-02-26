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
public class FederatedSumTest extends AutomatedTestBase {
	
	private final static String TEST_DIR = "functions/federated/";
	private final static String TEST_NAME = "FederatedSumTest";
	private final static String TEST_CLASS_DIR = TEST_DIR + FederatedSumTest.class.getSimpleName() + "/";
	
	private final static int port = 1222;
	private final static int blocksize = 1024;
	private int rows, cols;
	
	public FederatedSumTest(int rows, int cols) {
		this.rows = rows;
		this.cols = cols;
	}
	
	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		Object[][] data = new Object[][] {{1, 1000}, {10, 100}, {100, 10}, {1000, 1}, {10, 2000}, {2000, 10}};
		return Arrays.asList(data);
	}
	
	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME,
				new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] {"S.scalar", "R", "C"}));
	}
	
	@Test
	public void federatedSumCP() {
		federatedSum(Types.ExecMode.SINGLE_NODE);
	}
	
	@Test
	public void federatedSumSP() {
		federatedSum(Types.ExecMode.SPARK);
	}
	
	public void federatedSum(Types.ExecMode execMode) {
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		Types.ExecMode platformOld = rtplatform;
		
		Thread t = null;
		try {
			getAndLoadTestConfiguration(TEST_NAME);
			String HOME = SCRIPT_DIR + TEST_DIR;
			
			// empty script name because we don't execute any script, just start the worker
			fullDMLScriptName = "";
			programArgs = new String[] {"-w", Integer.toString(port)};
			
			double[][] A = getRandomMatrix(rows, cols, -10, 10, 1, 1);
			writeInputMatrixWithMTD("A", A, false, new MatrixCharacteristics(rows, cols, blocksize, rows * cols));
			t = new Thread(() -> runTest(true, false, null, -1));
			t.start();
			sleep(FED_WORKER_WAIT);
			
			// we need the reference file to not be written to hdfs, so we get the correct format
			rtplatform = Types.ExecMode.SINGLE_NODE;
			// Run reference dml script with normal matrix for Row/Col sum
			fullDMLScriptName = HOME + TEST_NAME + "Reference.dml";
			programArgs = new String[] {"-args", input("A"), input("A"), expected("R"), expected("C")};
			runTest(true, false, null, -1);
			
			// write expected sum
			double sum = 0;
			for (double[] doubles : A) {
				sum += Arrays.stream(doubles).sum();
			}
			sum *= 2;
			writeExpectedScalar("S", sum);
			
			// reference file should not be written to hdfs, so we set platform here
			rtplatform = execMode;
			if (rtplatform == Types.ExecMode.SPARK) {
				DMLScript.USE_LOCAL_SPARK_CONFIG = true;
			}
			TestConfiguration config = availableTestConfigurations.get(TEST_NAME);
			loadTestConfiguration(config);
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[] {"-explain", "-args", "\"localhost:" + port + "/" + input("A") + "\"",
				Integer.toString(rows), Integer.toString(cols), Integer.toString(rows * 2), output("S"),
				output("R"), output("C")};
			
			runTest(true, false, null, -1);
			
			// compare all sums via files
			compareResults(1e-11);
		}
		catch (InterruptedException e) {
			e.printStackTrace();
			assert (false);
		}
		finally {
			TestUtils.shutdownThread(t);
			rtplatform = platformOld;
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
		}
	}
}
