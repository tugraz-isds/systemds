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
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;

public class ParForImageMirrorTest extends AutomatedTestBase
{
	private final static String TEST_NAME = "parfor_image_mirror";
	private final static String TEST_DIR = "functions/parfor/";
	private static final String TEST_CLASS_DIR = TEST_DIR + ParForImageMirrorTest.class.getSimpleName() + "/";

	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME,new TestConfiguration(TEST_CLASS_DIR, TEST_NAME,new String[]{"B"}));
	}

	@Test
	public void testImageMirrorCP() {
		runImageMirrorTest(Types.ExecType.CP);
	}

	@Test
	public void testImageMirrorSP() {
		runImageMirrorTest(Types.ExecType.SPARK);
	}


	private void runImageMirrorTest(Types.ExecType instType)
	{
		Types.ExecMode platformOld = rtplatform;
		switch( instType ) {
			case SPARK: rtplatform = Types.ExecMode.SPARK; break;
			default: rtplatform = Types.ExecMode.HYBRID; break;
		}

		disableOutAndExpectedDeletion();

		try
		{
			loadTestConfiguration(getTestConfiguration(TEST_NAME));

			String inputFilename = input("A");
			String outputFilename = output("B");

			downloadInputDataset(inputFilename, "https://pjreddie.com/media/files/mnist_test.csv");

			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-explain","-stats", "-nvargs",
					"in_file=" + inputFilename,
					"out_file=" + outputFilename,
					"width=28",
					"height=28",
//					"num_images=10",
			};

			runTest(true, false, null, -1);
		}
		finally {
			rtplatform = platformOld;
		}
	}
}
