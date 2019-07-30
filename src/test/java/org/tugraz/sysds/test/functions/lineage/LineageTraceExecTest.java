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

package org.tugraz.sysds.test.functions.lineage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;
import org.tugraz.sysds.runtime.controlprogram.caching.MatrixObject;
import org.tugraz.sysds.runtime.instructions.cp.Data;
import org.tugraz.sysds.runtime.instructions.cp.ScalarObject;
import org.tugraz.sysds.runtime.lineage.Lineage;
import org.tugraz.sysds.runtime.lineage.LineageItem;
import org.tugraz.sysds.runtime.lineage.LineageItemUtils;
import org.tugraz.sysds.runtime.lineage.LineageParser;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class LineageTraceExecTest extends AutomatedTestBase {
	
	protected static final String TEST_DIR = "functions/lineage/";
	protected static final String TEST_NAME1 = "LineageTraceExec1"; //rand - matrix result
	protected static final String TEST_NAME2 = "LineageTraceExec2"; //rand - scalar result
	protected static final String TEST_NAME3 = "LineageTraceExec3"; //read - matrix result
	protected static final String TEST_NAME4 = "LineageTraceExec4"; //rand - matrix result - unspecified seed
	protected static final String TEST_NAME5 = "LineageTraceExec5"; //rand - scalar result - unspecified seed
	
	protected String TEST_CLASS_DIR = TEST_DIR + LineageTraceExecTest.class.getSimpleName() + "/";
	
	protected static final int numRecords = 10;
	protected static final int numFeatures = 5;
	
	public LineageTraceExecTest() {
		
	}
	
	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration( TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] {"R"}) );
		addTestConfiguration( TEST_NAME2, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2, new String[] {"R"}) );
		addTestConfiguration( TEST_NAME3, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME3, new String[] {"R"}) );
		addTestConfiguration( TEST_NAME4, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME4, new String[] {"R"}) );
		addTestConfiguration( TEST_NAME5, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME5, new String[] {"R"}) );
	}
	
	@Test
	public void testLineageTraceExec1() {
		testLineageTraceExec(TEST_NAME1);
	}
	
	@Test
	public void testLineageTraceExec2() {
		testLineageTraceExec(TEST_NAME2);
	}
	
	@Test
	public void testLineageTraceExec3() {
		testLineageTraceExec(TEST_NAME3);
	}
	
	@Test
	public void testLineageTraceExec4() {
		testLineageTraceExec(TEST_NAME4);
	}
	
	@Test
	public void testLineageTraceExec5() {
		testLineageTraceExec(TEST_NAME5);
	}
		
	private void testLineageTraceExec(String testname) {
		System.out.println("------------ BEGIN " + testname + "------------");
		
		getAndLoadTestConfiguration(testname);
		List<String> proArgs = new ArrayList<String>();
		
		proArgs.add("-explain");
		proArgs.add("-lineage");
		proArgs.add("-args");
		proArgs.add(input("X"));
		proArgs.add(output("R"));
		proArgs.add(String.valueOf(numRecords));
		proArgs.add(String.valueOf(numFeatures));
		programArgs = proArgs.toArray(new String[proArgs.size()]);
		fullDMLScriptName = getScript();
		
		if( testname.equals(TEST_NAME3) ) {
			double[][] X = getRandomMatrix(numRecords, numFeatures, 0, 1, 0.8, -1);
			writeInputMatrixWithMTD("X", X, true);
		}
		
		Lineage.resetObjects();
		//run the test
		runTest(true, EXCEPTION_NOT_EXPECTED, null, -1);
		
		//get lineage and generate program
		String Rtrace = readDMLLineageFromHDFS("R");
		LineageItem R = LineageParser.parseLineageTrace(Rtrace);
		Data ret = LineageItemUtils.computeByLineage(R);
		
		if( testname.equals(TEST_NAME2) || testname.equals(TEST_NAME5)) {
			double val1 = readDMLScalarFromHDFS("R").get(new CellIndex(1,1));
			double val2 = ((ScalarObject)ret).getDoubleValue();
			TestUtils.compareScalars(val1, val2, 1e-6);
		}
		else {
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("R");
			MatrixBlock tmp = ((MatrixObject)ret).acquireReadAndRelease();
			TestUtils.compareMatrices(dmlfile, tmp, 1e-6);
		}
	}
}
