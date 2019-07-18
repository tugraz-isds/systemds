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

package org.tugraz.sysds.runtime.instructions.cp;

import org.tugraz.sysds.common.Types.DataType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.data.TensorBlock;
import org.tugraz.sysds.runtime.instructions.InstructionUtils;
import org.tugraz.sysds.runtime.matrix.operators.AggregateUnaryOperator;
import org.tugraz.sysds.runtime.matrix.operators.Operator;

public class TensorAggregateUnaryCPInstruction extends UnaryCPInstruction
{
	private TensorAggregateUnaryCPInstruction(Operator op, CPOperand in, CPOperand out, String opcode, String istr) {
		this(op, in, null, null, out, opcode, istr);
	}

	protected TensorAggregateUnaryCPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand in3, CPOperand out,
	                                           String opcode, String istr) {
		super(CPType.TensorAggregateUnary, op, in1, in2, in3, out, opcode, istr);
	}
	
	public static TensorAggregateUnaryCPInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand out = new CPOperand(parts[2]);
		
		AggregateUnaryOperator aggun = InstructionUtils
			.parseBasicAggregateUnaryOperator(opcode, Integer.parseInt(parts[3]));
		return new TensorAggregateUnaryCPInstruction(aggun, in1, out, opcode, str);
	}
	
	@Override
	public void processInstruction( ExecutionContext ec ) {
		String output_name = output.getName();

		TensorBlock tensorBlock = ec.getTensorInput(input1.getName());
		AggregateUnaryOperator au_op = (AggregateUnaryOperator) _optr;

		// TODO use a generalized method on tensorBlock
		DoubleObject out = new DoubleObject(tensorBlock.sum());

		// TODO once cacheable tensorObjects are used release them
		//ec.releaseTensorInput(input1.getName());
		if(output.getDataType() == DataType.SCALAR){
			ec.setScalarOutput(output_name, out);
		} else{
			// TODO create a "temp" output tensor
			//ec.setTensorOutput(output_name, resultBlock);
			throw new DMLRuntimeException("Sum of tensor can currently always returns a Scalar");
		}
	}
}
