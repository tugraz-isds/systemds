/*
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

package org.tugraz.sysds.test.functions.paramserv;

import org.junit.Test;
import org.tugraz.sysds.parser.Statement;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;

@net.jcip.annotations.NotThreadSafe
public class ParamservLocalNNTest extends AutomatedTestBase {

	private static final String TEST_NAME = "paramserv-test";

	private static final String TEST_DIR = "functions/paramserv/";
	private static final String TEST_CLASS_DIR = TEST_DIR + ParamservLocalNNTest.class.getSimpleName() + "/";

	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] {}));
	}

	@Test
	public void testParamservBSPBatchDisjointContiguous() {
		runDMLTest(10, 3, Statement.PSUpdateType.BSP, Statement.PSFrequency.BATCH, 32, Statement.PSScheme.DISJOINT_CONTIGUOUS);
	}

	@Test
	public void testParamservASPBatch() {
		runDMLTest(10, 3, Statement.PSUpdateType.ASP, Statement.PSFrequency.BATCH, 32, Statement.PSScheme.DISJOINT_CONTIGUOUS);
	}

	@Test
	public void testParamservBSPEpoch() {
		runDMLTest(10, 3, Statement.PSUpdateType.BSP, Statement.PSFrequency.EPOCH, 32, Statement.PSScheme.DISJOINT_CONTIGUOUS);
	}

	@Test
	public void testParamservASPEpoch() {
		runDMLTest(10, 3, Statement.PSUpdateType.ASP, Statement.PSFrequency.EPOCH, 32, Statement.PSScheme.DISJOINT_CONTIGUOUS);
	}

	@Test
	public void testParamservBSPBatchDisjointRoundRobin() {
		runDMLTest(10, 3, Statement.PSUpdateType.BSP, Statement.PSFrequency.BATCH, 32, Statement.PSScheme.DISJOINT_ROUND_ROBIN);
	}

	@Test
	public void testParamservBSPBatchDisjointRandom() {
		runDMLTest(10, 3, Statement.PSUpdateType.BSP, Statement.PSFrequency.BATCH, 32, Statement.PSScheme.DISJOINT_RANDOM);
	}

	@Test
	public void testParamservBSPBatchOverlapReshuffle() {
		runDMLTest(10, 3, Statement.PSUpdateType.BSP, Statement.PSFrequency.BATCH, 32, Statement.PSScheme.OVERLAP_RESHUFFLE);
	}

	private void runDMLTest(int epochs, int workers, Statement.PSUpdateType utype, Statement.PSFrequency freq, int batchsize, Statement.PSScheme scheme) {
		TestConfiguration config = getTestConfiguration(ParamservLocalNNTest.TEST_NAME);
		loadTestConfiguration(config);
		programArgs = new String[] { "-explain", "-stats", "-nvargs", "mode=LOCAL", "epochs=" + epochs,
			"workers=" + workers, "utype=" + utype, "freq=" + freq, "batchsize=" + batchsize,
			"scheme=" + scheme };
		String HOME = SCRIPT_DIR + TEST_DIR;
		fullDMLScriptName = HOME + ParamservLocalNNTest.TEST_NAME + ".dml";
		runTest(true, false, null, null, -1);
	}
}
