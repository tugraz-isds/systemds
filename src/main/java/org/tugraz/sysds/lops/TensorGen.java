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

package org.tugraz.sysds.lops;

import org.tugraz.sysds.common.Types;

public class TensorGen extends Lop {
	public static final String TENSOR_OPCODE = "tensor";

	public TensorGen(Lop[] input, Types.ValueType vt, LopProperties.ExecType et) {
		super(Type.TensorGen, Types.DataType.TENSOR, vt);
		for(Lop in : input) {
			this.addInput(in);
			in.addOutput(this);
		}
		lps.setProperties(inputs, et);
	}

	@Override
	public String toString() {
		return " Operation: " + TENSOR_OPCODE;
	}

	//CP instruction
	@Override
	public String getInstructions(String input1, String input2, String input3, String output) {
		StringBuilder sb = new StringBuilder();
		sb.append( getExecType() );

		sb.append( OPERAND_DELIMITOR );
		sb.append( TENSOR_OPCODE );
		sb.append( OPERAND_DELIMITOR );
		sb.append( getInputs().get(0).prepInputOperand(input1));

		//dims, byrow
		String[] inputX = new String[]{input2,input3};
		for( int i=1; i<=(inputX.length); i++ ) {
			Lop ltmp = getInputs().get(i);
			sb.append( OPERAND_DELIMITOR );
			sb.append( ltmp.prepScalarInputOperand(getExecType()));
		}

		//output
		sb.append( OPERAND_DELIMITOR );
		sb.append( this.prepOutputOperand(output));

		if( getExecType()== LopProperties.ExecType.SPARK ) {
			sb.append( OPERAND_DELIMITOR );
			// TODO deactivate output empty block?
			sb.append( true );
		}

		return sb.toString();
	}
}
