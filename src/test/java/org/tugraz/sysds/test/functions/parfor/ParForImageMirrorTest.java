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
 */

package org.tugraz.sysds.test.functions.parfor;

import org.junit.Test;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

import java.util.HashMap;

public class ParForImageMirrorTest extends AutomatedTestBase
{
	private final static String TEST_NAME = "parfor_image_mirror";
	private final static String TEST_DIR = "functions/parfor/";
	private static final String TEST_CLASS_DIR = TEST_DIR + ParForImageMirrorTest.class.getSimpleName() + "/";

	private final static double spSparse = 0.1;
	private final static double spDense = 0.9;
	private final static int image_width = 32;
	private final static int image_height = 32;
	private final static int rows = 128; // -> number of images
	private final static int cols = image_width * image_height;
	private final static double eps = 1e-10;

	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME,new TestConfiguration(TEST_CLASS_DIR, TEST_NAME,new String[]{"B"}));
	}

	@Test
	public void testImageMirrorDenseCP() {
		runImageMirrorTest(false, Types.ExecType.CP);
	}

	@Test
	public void testImageMirrorDenseSP() { runImageMirrorTest( false, Types.ExecType.SPARK);	}

	@Test
	public void testImageMirrorSparseCP() {
		runImageMirrorTest(true, Types.ExecType.CP);
	}

	@Test
	public void testImageMirrorSparseSP() { runImageMirrorTest( true, Types.ExecType.SPARK); }


	private void runImageMirrorTest(boolean sparse, Types.ExecType instType)
	{
		Types.ExecMode platformOld = rtplatform;
		switch( instType ) {
			case SPARK: rtplatform = Types.ExecMode.SPARK; break;
			default: rtplatform = Types.ExecMode.HYBRID; break;
		}

		double sparsity = sparse ? spSparse : spDense;

		try
		{
			loadTestConfiguration(getTestConfiguration(TEST_NAME));

			//generate actual dataset
			double[][] A = getRandomMatrix(rows, cols, 0, 255, sparsity, 7);
			writeInputMatrixWithMTD("A", A, true);


			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-explain","-stats", "-nvargs",
					"in_file=" + input("A"),
					"out_file=" + output("B"),
					"width=" + image_width,
					"height=" + image_height,
			};

			fullRScriptName = HOME + TEST_NAME + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " + expectedDir()
					+ " " + image_width + " " + image_height;

			runTest(true, false, null, -1);

			runRScript(true);

			//compare matrices
			HashMap<MatrixValue.CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("B");
			HashMap<MatrixValue.CellIndex, Double> rfile  = readRMatrixFromFS("B");
			TestUtils.compareMatrices(dmlfile, rfile, eps, "Stat-DML", "Stat-R");

		}
		finally {
			rtplatform = platformOld;
		}
	}
}
