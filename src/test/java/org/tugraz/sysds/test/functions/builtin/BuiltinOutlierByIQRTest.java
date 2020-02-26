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

import org.junit.Test;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.lops.LopProperties;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;

import java.util.concurrent.ThreadLocalRandom;

public class BuiltinOutlierByIQRTest extends AutomatedTestBase {
	private final static String TEST_NAME = "outlier_by_IQR";
	private final static String TEST_DIR = "functions/builtin/";
	private static final String TEST_CLASS_DIR = TEST_DIR + BuiltinOutlierByIQRTest.class.getSimpleName() + "/";

	private final static int rows = 100;
	private final static int cols = 15;
	private final static double spDense = 0.7;
	private final static double spSparse = 0.8;

	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME,new TestConfiguration(TEST_CLASS_DIR, TEST_NAME,new String[]{"B"}));
	}

	@Test
	public void testOutlierRepair0CP() {
		runOutlierTest(false, 1.5, 0, 10,LopProperties.ExecType.CP);
	}

	@Test
	public void testOutlierRepair1CP() {
		runOutlierTest(false, 2, 1, 10,LopProperties.ExecType.CP);
	}


	@Test
	public void testOutlierRepair0SP() {
		runOutlierTest(false, 2, 0, 10,LopProperties.ExecType.SPARK);
	}

	@Test
	public void testOutlierRepair1SP() {
		runOutlierTest(false, 1.5, 1, 10,LopProperties.ExecType.SPARK);
	}
	@Test
	public void testOutlierRepair0IterativeCP() {
		runOutlierTest(false, 1.5, 0, 0,LopProperties.ExecType.CP);
	}

	@Test
	public void testOutlierRepair1IterativeCP() {
		runOutlierTest(false, 1.5, 1, 0,LopProperties.ExecType.CP);
	}


	@Test
	public void testOutlierRepair0IterativeSP() {
		runOutlierTest(false, 1.5, 0, 0,LopProperties.ExecType.SPARK);
	}

	@Test
	public void testOutlierRepair1IterativeSP() {
		runOutlierTest(false, 1.5, 1, 0,LopProperties.ExecType.SPARK);
	}


	private void runOutlierTest(boolean sparse, double  k,  int repair, int max_iterations, LopProperties.ExecType instType)
	{
		Types.ExecMode platformOld = setExecMode(instType);

		try
		{
			loadTestConfiguration(getTestConfiguration(TEST_NAME));

			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{ "-args", input("A"), String.valueOf(k),
					 String.valueOf(repair), String.valueOf(max_iterations),output("B")};

			//generate actual dataset
			double[][] A =  getRandomMatrix(rows, cols, 1, 100, sparse?spSparse:spDense, 10);
			for(int i=0; i<A.length/4; i++) {
				int r = ThreadLocalRandom.current().nextInt(0, A.length);
				int c = ThreadLocalRandom.current().nextInt(0, A[0].length);
				double badValue = ThreadLocalRandom.current().nextDouble(0, A.length*100);
				A[r][c] = badValue;
			}

			writeInputMatrixWithMTD("A", A, true);

			runTest(true, false, null, -1);

		}
		finally {
			rtplatform = platformOld;
		}
	}
}