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

import org.junit.Test;
import org.tugraz.sysds.api.DMLException;
import org.tugraz.sysds.api.jmlc.Connection;
import org.tugraz.sysds.api.jmlc.PreparedScript;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.test.AutomatedTestBase;

public class InputToStringTest extends AutomatedTestBase 
{
	@Override
	public void setUp() { }
	
	@Test
	public void testScalartoString() throws DMLException {
		try( Connection conn = new Connection() ) {
			PreparedScript pscript = conn.prepareScript(
				"s = read(\"tmp\", data_type=\"scalar\"); print(toString(s));",
				new String[]{"s"}, new String[]{});
			pscript.setScalar("s", 7);
			pscript.executeScript();
		}
	}
	
	@Test
	public void testMatrixtoString() throws DMLException {
		try( Connection conn = new Connection() ) {
			PreparedScript pscript = conn.prepareScript(
				"m = read(\"tmp\", data_type=\"matrix\"); print(toString(m));",
				new String[]{"m"}, new String[]{});
			pscript.setMatrix("m", MatrixBlock.randOperations(7, 3, 1.0, 0, 1, "uniform", 7), false);
			pscript.executeScript();
		}
	}
	
	@Test
	public void testFrametoString() throws DMLException {
		try( Connection conn = new Connection() ) {
			PreparedScript pscript = conn.prepareScript(
				"f = read(\"tmp\", data_type=\"frame\"); print(toString(f));",
				new String[]{"f"}, new String[]{});
			pscript.setFrame("f", DataConverter.convertToFrameBlock(
				MatrixBlock.randOperations(7, 3, 1.0, 0, 1, "uniform", 7)), false);
			pscript.executeScript();
		}
	}
}
