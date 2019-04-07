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

package org.tugraz.sysds.test.integration.applications.dml;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.tugraz.sysds.test.integration.applications.LineageTraceSelfReferenceTest;


@RunWith(value = Parameterized.class)
public class LineageTraceSelfReferenceDMLTest extends LineageTraceSelfReferenceTest {
	
	public LineageTraceSelfReferenceDMLTest(int rows, int cols) {
		super(rows, cols);
		TEST_CLASS_DIR = TEST_DIR + LineageTraceSelfReferenceDMLTest.class.getSimpleName() + "/";
	}
	
	@Test
	public void testLineageTraceDml() {
		testLineageTrace();
	}
}
