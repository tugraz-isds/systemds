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
 *
 */

package org.tugraz.sysds.runtime.instructions.fed;

import org.tugraz.sysds.common.Types.DataType;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.caching.MatrixObject;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.functionobjects.Multiply;
import org.tugraz.sysds.runtime.functionobjects.Plus;
import org.tugraz.sysds.runtime.instructions.InstructionUtils;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;
import org.tugraz.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.tugraz.sysds.runtime.matrix.operators.AggregateOperator;
import org.tugraz.sysds.runtime.matrix.operators.Operator;

public class AggregateBinaryFEDInstruction extends BinaryFEDInstruction {
	
	public AggregateBinaryFEDInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, String opcode,
			String istr) {
		super(FEDType.AggregateBinary, op, in1, in2, out, opcode, istr);
	}
	
	public static AggregateBinaryFEDInstruction parseInstruction(String str) {
		CPOperand in1 = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
		CPOperand in2 = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
		CPOperand out = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
		
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		
		if( !opcode.equalsIgnoreCase("ba+*") ) {
			throw new DMLRuntimeException("AggregateBinaryInstruction.parseInstruction():: Unknown opcode " + opcode);
		}
		
		InstructionUtils.checkNumFields(parts, 4);
		in1.split(parts[1]);
		in2.split(parts[2]);
		out.split(parts[3]);
		int k = Integer.parseInt(parts[4]);
		
		AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
		AggregateBinaryOperator aggbin = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg, k);
		return new AggregateBinaryFEDInstruction(aggbin, in1, in2, out, opcode, str);
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) {
		//get inputs
		MatrixObject mo1 = ec.getMatrixObject(input1.getName());
		MatrixObject mo2 = ec.getMatrixObject(input2.getName());
		MatrixObject out = ec.getMatrixObject(output.getName());
		
		// TODO compute matrix multiplication
		// compute matrix-vector multiplication
		AggregateBinaryOperator ab_op = (AggregateBinaryOperator) _optr;
		MatrixObject ret;
		if (mo1.isFederated()) {
			mo1.federatedAggregateBinary(mo2, out, ab_op, false);
		}
		else if (mo2.isFederated()) {
			mo2.federatedAggregateBinary(mo1, out, ab_op, true);
		}
		else
			throw new DMLRuntimeException("Federated instruction on non federated data");
	}
}
