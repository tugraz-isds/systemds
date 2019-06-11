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

import org.tugraz.sysds.common.Types.DataType;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.instructions.InstructionUtils;
import org.tugraz.sysds.runtime.lineage.Lineage;
import org.tugraz.sysds.runtime.lineage.LineageItem;
import org.tugraz.sysds.runtime.matrix.data.LibCommonsMath;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.operators.Operator;

public class MultiReturnBuiltinCPInstruction extends ComputationCPInstruction {

	protected ArrayList<CPOperand> _outputs;

	private MultiReturnBuiltinCPInstruction(Operator op, CPOperand input1, ArrayList<CPOperand> outputs, String opcode,
			String istr) {
		super(CPType.MultiReturnBuiltin, op, input1, null, outputs.get(0), opcode, istr);
		_outputs = outputs;
	}
	
	public CPOperand getOutput(int i) {
		return _outputs.get(i);
	}
	
	public static MultiReturnBuiltinCPInstruction parseInstruction ( String str ) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		ArrayList<CPOperand> outputs = new ArrayList<>();
		// first part is always the opcode
		String opcode = parts[0];
		
		if ( opcode.equalsIgnoreCase("qr") ) {
			// one input and two ouputs
			CPOperand in1 = new CPOperand(parts[1]);
			outputs.add ( new CPOperand(parts[2], ValueType.FP64, DataType.MATRIX) );
			outputs.add ( new CPOperand(parts[3], ValueType.FP64, DataType.MATRIX) );
			
			return new MultiReturnBuiltinCPInstruction(null, in1, outputs, opcode, str);
		}
		else if ( opcode.equalsIgnoreCase("lu") ) {
			CPOperand in1 = new CPOperand(parts[1]);
			
			// one input and three outputs
			outputs.add ( new CPOperand(parts[2], ValueType.FP64, DataType.MATRIX) );
			outputs.add ( new CPOperand(parts[3], ValueType.FP64, DataType.MATRIX) );
			outputs.add ( new CPOperand(parts[4], ValueType.FP64, DataType.MATRIX) );
			
			return new MultiReturnBuiltinCPInstruction(null, in1, outputs, opcode, str);
			
		}
		else if ( opcode.equalsIgnoreCase("eigen") ) {
			// one input and two outputs
			CPOperand in1 = new CPOperand(parts[1]);
			outputs.add ( new CPOperand(parts[2], ValueType.FP64, DataType.MATRIX) );
			outputs.add ( new CPOperand(parts[3], ValueType.FP64, DataType.MATRIX) );
			
			return new MultiReturnBuiltinCPInstruction(null, in1, outputs, opcode, str);
			
		}
		else if ( opcode.equalsIgnoreCase("svd") ) {
			CPOperand in1 = new CPOperand(parts[1]);

			// one input and three outputs
			outputs.add ( new CPOperand(parts[2], ValueType.FP64, DataType.MATRIX) );
			outputs.add ( new CPOperand(parts[3], ValueType.FP64, DataType.MATRIX) );
			outputs.add ( new CPOperand(parts[4], ValueType.FP64, DataType.MATRIX) );
			
			return new MultiReturnBuiltinCPInstruction(null, in1, outputs, opcode, str);

		}
		else {
			throw new DMLRuntimeException("Invalid opcode in MultiReturnBuiltin instruction: " + opcode);
		}

	}

	@Override 
	public void processInstruction(ExecutionContext ec) {
		if(!LibCommonsMath.isSupportedMultiReturnOperation(getOpcode()))
			throw new DMLRuntimeException("Invalid opcode in MultiReturnBuiltin instruction: " + getOpcode());
		
		MatrixBlock in = ec.getMatrixInput(input1.getName());
		MatrixBlock[] out = LibCommonsMath.multiReturnOperations(in, getOpcode());
		ec.releaseMatrixInput(input1.getName(), getExtendedOpcode());
		for(int i=0; i < _outputs.size(); i++) {
			ec.setMatrixOutput(_outputs.get(i).getName(), out[i], getExtendedOpcode());
		}
	}
	
	@Override
	public LineageItem[] getLineageItems() {
		ArrayList<LineageItem> lineages = new ArrayList<>();
		if (input1 != null)
			lineages.add(Lineage.getOrCreate(input1));
		if (input2 != null)
			lineages.add(Lineage.getOrCreate(input2));
		if (input3 != null)
			lineages.add(Lineage.getOrCreate(input3));
		
		ArrayList<LineageItem> items = new ArrayList<>();
		for (CPOperand out : _outputs) {
			items.add(new LineageItem(out.getName(),
					getOpcode(), lineages.toArray(new LineageItem[0])));
		}
		return items.toArray(new LineageItem[items.size()]);
	}
}
