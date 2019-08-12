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

package org.tugraz.sysds.test.functions.builtin;

import org.tugraz.sysds.common.Types;


import org.junit.Test;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;

import java.io.File;
//package io;
import java.nio.file.*;
import java.io.*;
import java.util.*;

public class BuiltinSliceFinderTest extends AutomatedTestBase {

	private final static String TEST_NAME = "slicefinder";
	private final static String TEST_DIR = "functions/builtin/";
	private static final String TEST_CLASS_DIR = TEST_DIR + BuiltinSliceFinderTest.class.getSimpleName() + "/";

    /*private final static double eps = 1e-10;
    private final static int rows = 10;
    private final static int cols = 3;
    private final static double spSparse = 0.3;
    private final static double spDense = 0.7;*/


	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[]{"B"}));
	}

	@Test
	public void testslicefinder() {
		try {
			runslicefindertest();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void runslicefindertest(){
		//ExecMode platformOld = setExecMode(instType);
		//String X = null;
		String dml_test_name = TEST_NAME;

		try
		{
			loadTestConfiguration(getTestConfiguration(TEST_NAME));
			String HOME = SCRIPT_DIR + TEST_DIR;

			fullDMLScriptName = HOME + dml_test_name + ".dml";
			//programArgs = new String[]{"-explain", "-args", input("A"), input("B"), output("C") };
			//fullRScriptName = HOME + TEST_NAME + ".R";
			//rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " "  + expectedDir();

			runTest();

		}
		finally {
			//rtplatform = platformOld;
		}

	}

}



