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

import org.junit.Test;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.runtime.lineage.Lineage;
import org.tugraz.sysds.runtime.lineage.LineageItem;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestUtils;

import java.util.ArrayList;
import java.util.List;

public class LineageReadTest extends AutomatedTestBase {
	
	protected static final String TEST_DIR = "functions/lineage/";
	protected static final String TEST_NAME = "LineageRead";
	protected String TEST_CLASS_DIR = TEST_DIR + LineageReadTest.class.getSimpleName() + "/";
	
	@Override
	public void setUp() {
		addTestConfiguration(TEST_CLASS_DIR, TEST_NAME);
	}
	
	@Test
	public void testLineageRead() {
		OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = false;
		OptimizerUtils.ALLOW_SUM_PRODUCT_REWRITES = false;
		
		getAndLoadTestConfiguration(TEST_NAME);
		
		String lineage =
				"(0) target/testTemp/functions/lineage/LineageTraceTest/in/X\n" +
						"(1) false\n" +
						"(2) MATRIX\n" +
						"(3) [10 x 5, nnz=-1 (false), blocks (-1 x -1)]\n" +
						"(4) createvar (0) (1) (2) (3)\n" +
						"(10) rblk (4)\n" +
						"(16) 3\n" +
						"(17) * (10) (16)\n" +
						"(23) 5\n" +
						"(24) + (17) (23)\n" +
						"(31) target/testTemp/functions/lineage/LineageTraceTest/out/X\n" +
						"(32) textcell\n" +
						"(33) write (24) (31) (32)\n";
		
		LineageItem li = Lineage.parseLineage(lineage);
//		should i can process this?
//		How to test this? Comparing with output?I
	}
}
