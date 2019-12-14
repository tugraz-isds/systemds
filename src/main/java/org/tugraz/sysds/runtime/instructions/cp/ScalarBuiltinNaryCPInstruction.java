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

package org.tugraz.sysds.runtime.instructions.cp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.lineage.LineageItem;
import org.tugraz.sysds.runtime.lineage.LineageItemUtils;
import org.tugraz.sysds.runtime.lineage.LineageTraceable;
import org.tugraz.sysds.runtime.matrix.operators.Operator;

/**
 * The ScalarBuiltinMultipleCPInstruction class is responsible for printf-style
 * Java-based string formatting. The first input is the format string. The
 * inputs after the first input are the arguments to be formatted in the format
 * string.
 *
 */
public class ScalarBuiltinNaryCPInstruction extends BuiltinNaryCPInstruction implements LineageTraceable {

	protected ScalarBuiltinNaryCPInstruction(Operator op, String opcode, String istr, CPOperand output, CPOperand[] inputs) {
		super(op, opcode, istr, output, inputs);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		if( "printf".equals(getOpcode()) ) {
			List<ScalarObject> scalarObjects = new ArrayList<>();
			for (CPOperand input : inputs) {
				ScalarObject so = ec.getScalarInput(input);
				scalarObjects.add(so);
			}

			// determine the format string (first argument) to pass to String.format
			ScalarObject formatStringObject = scalarObjects.get(0);
			if (formatStringObject.getValueType() != ValueType.STRING) {
				throw new DMLRuntimeException("First parameter needs to be a string");
			}
			String formatString = formatStringObject.getStringValue();

			// determine the arguments after the format string to pass to String.format
			Object[] objects = null;
			if (scalarObjects.size() > 1) {
				objects = new Object[scalarObjects.size() - 1];
				for (int i = 1; i < scalarObjects.size(); i++) {
					ScalarObject scalarObject = scalarObjects.get(i);
					switch (scalarObject.getValueType()) {
					case INT64:
						objects[i - 1] = scalarObject.getLongValue();
						break;
					case FP64:
						objects[i - 1] = scalarObject.getDoubleValue();
						break;
					case BOOLEAN:
						objects[i - 1] = scalarObject.getBooleanValue();
						break;
					case STRING:
						objects[i - 1] = scalarObject.getStringValue();
						break;
					default:
					}
				}
			}

			String result = String.format(formatString, objects);
			if (!DMLScript.suppressPrint2Stdout()) {
				System.out.println(result);
			}
			
			ec.setScalarOutput(output.getName(), new StringObject(result));
		}
		else if( "list".equals(getOpcode()) ) {
			//obtain all input data objects, incl handling of literals
			List<Data> data = (inputs== null) ? new ArrayList<>() :
				Arrays.stream(inputs).map(in -> ec.getVariable(in)).collect(Collectors.toList());
			
			//create list object over all inputs
			ListObject list = new ListObject(data);
			list.deriveAndSetStatusFromData();
			
			ec.setVariable(output.getName(), list);
		}
		else {
			throw new DMLRuntimeException("Opcode (" + getOpcode() 
				+ ") not recognized in ScalarBuiltinMultipleCPInstruction");
		}

	}
	
	@Override
	public LineageItem[] getLineageItems(ExecutionContext ec) {
		return new LineageItem[]{new LineageItem(output.getName(),
			instString, getOpcode())};
	}

}
