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

package org.tugraz.sysds.test.functions.jmlc;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.tugraz.sysds.api.DMLException;
import org.tugraz.sysds.api.jmlc.Connection;
import org.tugraz.sysds.api.jmlc.PreparedScript;
import org.tugraz.sysds.runtime.instructions.cp.ScalarObject;
import org.tugraz.sysds.test.AutomatedTestBase;

/**
 * Test input and output capabilities of JMLC API.
 *
 */
public class JMLCInputOutputTest extends AutomatedTestBase {
	private final static String TEST_NAME = "JMLCInputOutputTest";
	private final static String TEST_DIR = "functions/jmlc/";

	@Override
	public void setUp() {
		addTestConfiguration(TEST_DIR, TEST_NAME);
		getAndLoadTestConfiguration(TEST_NAME);
	}

	@Test
	public void testScalarInputInt() throws IOException, DMLException {
		Connection conn = new Connection();
		String str = conn.readScript(baseDirectory + File.separator + "scalar-input.dml");
		PreparedScript script = conn.prepareScript(str, new String[] { "inScalar1", "inScalar2" }, new String[] {});
		int inScalar1 = 2;
		int inScalar2 = 3;
		script.setScalar("inScalar1", inScalar1);
		script.setScalar("inScalar2", inScalar2);

		setExpectedStdOut("total:5");
		script.executeScript();
		conn.close();
	}

	@Test
	public void testScalarInputDouble() throws IOException, DMLException {
		Connection conn = new Connection();
		String str = conn.readScript(baseDirectory + File.separator + "scalar-input.dml");
		PreparedScript script = conn.prepareScript(str, new String[] { "inScalar1", "inScalar2" }, new String[] {});
		double inScalar1 = 3.5;
		double inScalar2 = 4.5;
		script.setScalar("inScalar1", inScalar1);
		script.setScalar("inScalar2", inScalar2);

		setExpectedStdOut("total:8.0");
		script.executeScript();
		conn.close();
	}

	@Test
	public void testScalarInputBoolean() throws IOException, DMLException {
		Connection conn = new Connection();
		String str = conn.readScript(baseDirectory + File.separator + "scalar-input.dml");
		PreparedScript script = conn.prepareScript(str, new String[] { "inScalar1", "inScalar2" }, new String[] {});
		boolean inScalar1 = true;
		boolean inScalar2 = true;
		script.setScalar("inScalar1", inScalar1);
		script.setScalar("inScalar2", inScalar2);

		setExpectedStdOut("total:2.0");
		script.executeScript();
		conn.close();
	}

	@Test
	public void testScalarInputLong() throws IOException, DMLException {
		Connection conn = new Connection();
		String str = conn.readScript(baseDirectory + File.separator + "scalar-input.dml");
		PreparedScript script = conn.prepareScript(str, new String[] { "inScalar1", "inScalar2" }, new String[] {});
		long inScalar1 = 4;
		long inScalar2 = 5;
		script.setScalar("inScalar1", inScalar1);
		script.setScalar("inScalar2", inScalar2);

		setExpectedStdOut("total:9");
		script.executeScript();
		conn.close();
	}

	@Test
	public void testScalarInputString() throws IOException, DMLException {
		Connection conn = new Connection();
		String str = conn.readScript(baseDirectory + File.separator + "scalar-input.dml");
		PreparedScript script = conn.prepareScript(str, new String[] { "inScalar1", "inScalar2" }, new String[] {});
		String inScalar1 = "Plant";
		String inScalar2 = " Trees";
		script.setScalar("inScalar1", inScalar1);
		script.setScalar("inScalar2", inScalar2);

		setExpectedStdOut("total:Plant Trees");
		script.executeScript();
		conn.close();
	}

	@Test
	public void testScalarInputStringExplicitValueType() throws IOException, DMLException {
		Connection conn = new Connection();
		String str = conn.readScript(baseDirectory + File.separator + "scalar-input-string.dml");
		PreparedScript script = conn.prepareScript(str, new String[] { "inScalar1", "inScalar2" }, new String[] {});
		String inScalar1 = "hello";
		String inScalar2 = "goodbye";
		script.setScalar("inScalar1", inScalar1);
		script.setScalar("inScalar2", inScalar2);

		setExpectedStdOut("result:hellogoodbye");
		script.executeScript();
		conn.close();
	}

	@Test
	public void testScalarOutputLong() throws DMLException {
		Connection conn = new Connection();
		String str = "outInteger = 5;\nwrite(outInteger, './tmp/outInteger');";
		PreparedScript script = conn.prepareScript(str, new String[] {}, new String[] { "outInteger" });

		long result = script.executeScript().getLong("outInteger");
		Assert.assertEquals(5, result);
		conn.close();
	}

	@Test
	public void testScalarOutputDouble() throws DMLException {
		Connection conn = new Connection();
		String str = "outDouble = 1.23;\nwrite(outDouble, './tmp/outDouble');";
		PreparedScript script = conn.prepareScript(str, new String[] {}, new String[] { "outDouble" });

		double result = script.executeScript().getDouble("outDouble");
		Assert.assertEquals(1.23, result, 0);
		conn.close();
	}

	@Test
	public void testScalarOutputString() throws DMLException {
		Connection conn = new Connection();
		String str = "outString = 'hello';\nwrite(outString, './tmp/outString');";
		PreparedScript script = conn.prepareScript(str, new String[] {}, new String[] { "outString" });

		String result = script.executeScript().getString("outString");
		Assert.assertEquals("hello", result);
		conn.close();
	}

	@Test
	public void testScalarOutputBoolean() throws DMLException {
		Connection conn = new Connection();
		String str = "outBoolean = FALSE;\nwrite(outBoolean, './tmp/outBoolean');";
		PreparedScript script = conn.prepareScript(str, new String[] {}, new String[] { "outBoolean" });

		boolean result = script.executeScript().getBoolean("outBoolean");
		Assert.assertEquals(false, result);
		conn.close();
	}

	@Test
	public void testScalarOutputScalarObject() throws DMLException {
		Connection conn = new Connection();
		String str = "outDouble = 1.23;\nwrite(outDouble, './tmp/outDouble');";
		PreparedScript script = conn.prepareScript(str, new String[] {}, new String[] { "outDouble" });

		ScalarObject so = script.executeScript().getScalarObject("outDouble");
		double result = so.getDoubleValue();
		Assert.assertEquals(1.23, result, 0);
		conn.close();
	}

}