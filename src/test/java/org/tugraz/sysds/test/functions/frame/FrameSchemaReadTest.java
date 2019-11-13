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

package org.tugraz.sysds.test.functions.frame;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;
import org.tugraz.sysds.parser.DataExpression;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.io.FrameReader;
import org.tugraz.sysds.runtime.io.FrameReaderBinaryBlock;
import org.tugraz.sysds.runtime.io.FrameReaderFactory;
import org.tugraz.sysds.runtime.io.FrameWriter;
import org.tugraz.sysds.runtime.io.FrameWriterFactory;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.runtime.matrix.data.InputInfo;
import org.tugraz.sysds.runtime.matrix.data.OutputInfo;
import org.tugraz.sysds.runtime.util.UtilFunctions;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class FrameSchemaReadTest extends AutomatedTestBase
{
	private final static String TEST_DIR = "functions/frame/";
	private final static String TEST_NAME1 = "FrameSchemaRead1";
	private final static String TEST_NAME2 = "FrameSchemaRead2";
	private final static String TEST_CLASS_DIR = TEST_DIR + FrameSchemaReadTest.class.getSimpleName() + "/";
	

	private final static int rows = 1593;
	private final static ValueType[] schemaStrings = new ValueType[]{ValueType.STRING, ValueType.STRING, ValueType.STRING};	
	private final static ValueType[] schemaDoubles = new ValueType[]{ValueType.FP64, ValueType.FP64};	
	private final static ValueType[] schemaMixed = new ValueType[]{ValueType.STRING, ValueType.FP64, ValueType.INT64, ValueType.BOOLEAN};	
	
	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] {"B"}));
		addTestConfiguration(TEST_NAME2, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2, new String[] {"B"}));		
	}
	
	@Test
	public void testFrameStringsSchemaSpecRead()  {
		runFrameSchemaReadTest(TEST_NAME1, schemaStrings, false);
	}
	
	@Test
	public void testFrameStringsSchemaWildcardRead()  {
		runFrameSchemaReadTest(TEST_NAME1, schemaStrings, true);
	}
	
	@Test
	public void testFrameStringsNoSchemaRead()  {
		runFrameSchemaReadTest(TEST_NAME2, schemaStrings, false);
	}
	
	@Test
	public void testFrameDoublesSchemaSpecRead()  {
		runFrameSchemaReadTest(TEST_NAME1, schemaDoubles, false);
	}
	
	@Test
	public void testFrameDoublesSchemaWildcardRead()  {
		runFrameSchemaReadTest(TEST_NAME1, schemaDoubles, true);
	}
	
	@Test
	public void testFrameDoublesNoSchemaRead()  {
		runFrameSchemaReadTest(TEST_NAME2, schemaDoubles, false);
	}
	
	@Test
	public void testFrameMixedSchemaSpecRead()  {
		runFrameSchemaReadTest(TEST_NAME1, schemaMixed, false);
	}
	
	@Test
	public void testFrameMixedSchemaWildcardRead()  {
		runFrameSchemaReadTest(TEST_NAME1, schemaMixed, true);
	}
	
	@Test
	public void testFrameMixedNoSchemaRead()  {
		runFrameSchemaReadTest(TEST_NAME2, schemaMixed, false);
	}
	

	
	/**
	 * 
	 * @param testname
	 * @param schema
	 * @param wildcard
	 */
	private void runFrameSchemaReadTest( String testname, ValueType[] schema, boolean wildcard)
	{
		try
		{
			TestConfiguration config = getTestConfiguration(testname);
			loadTestConfiguration(config);
			
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + testname + ".dml";
			programArgs = new String[]{"-explain","-args", input("A"), getSchemaString(schema, wildcard), 
					Integer.toString(rows), Integer.toString(schema.length), output("B") };
			
			//data generation
			double[][] A = getRandomMatrix(rows, schema.length, -10, 10, 0.9, 2373); 
			
			//prepare input/output infos
			FrameBlock frame1 = new FrameBlock(schema);
			initFrameData(frame1, A, schema);
			
			//write frame data to hdfs
			FrameWriter writer = FrameWriterFactory.createFrameWriter(OutputInfo.CSVOutputInfo);
			writer.writeFrameToHDFS(frame1, input("A"), rows, schema.length);

			//run testcase
			runTest(true, false, null, -1);
			
			//read frame data from hdfs (not via readers to test physical schema)
			FrameReader reader = FrameReaderFactory.createFrameReader(InputInfo.BinaryBlockInputInfo);
			FrameBlock frame2 = ((FrameReaderBinaryBlock)reader).readFirstBlock(output("B"));
			
			//verify output schema
			ValueType[] schemaExpected = (testname.equals(TEST_NAME2) || wildcard) ?
					Collections.nCopies(schema.length, ValueType.STRING).toArray(new ValueType[0]) : schema;
			for( int i=0; i<schemaExpected.length; i++ ) {
				Assert.assertEquals("Wrong result: "+frame2.getSchema()[i]+".",
						schemaExpected[i], frame2.getSchema()[i]);
			}
		}
		catch(Exception ex) {
			ex.printStackTrace();
			throw new RuntimeException(ex);
		}
	}
	
	private static void initFrameData(FrameBlock frame, double[][] data, ValueType[] lschema) {
		Object[] row1 = new Object[lschema.length];
		for( int i=0; i<rows; i++ ) {
			for( int j=0; j<lschema.length; j++ )
				data[i][j] = UtilFunctions.objectToDouble(lschema[j], 
						row1[j] = UtilFunctions.doubleToObject(lschema[j], data[i][j]));
			frame.appendRow(row1);
		}
	}
	
	private static String getSchemaString( ValueType[] lschema, boolean wildcard ) {
		if( wildcard )
			return "*";		
		StringBuilder ret = new StringBuilder();
		for( ValueType vt : lschema ) {
			if( ret.length()>0 )
				ret.append(DataExpression.DEFAULT_DELIM_DELIMITER);
			ret.append(vt.toString().toLowerCase());
		}
		return ret.toString();
	}
}
