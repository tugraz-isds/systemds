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

import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.instructions.InstructionUtils;
import org.tugraz.sysds.runtime.lineage.LineageItem;
import org.tugraz.sysds.runtime.matrix.operators.Operator;

public final class ListAppendRemoveCPInstruction extends AppendCPInstruction {

	private CPOperand output2 = null;
	
	protected ListAppendRemoveCPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out,
			AppendType type, String opcode, String istr) {
		super(op, in1, in2, out, type, opcode, istr);
		if( opcode.equals("remove") )
			output2 = new CPOperand(InstructionUtils.getInstructionPartsWithValueType(istr)[4]);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		//get input list and data
		ListObject lo = ec.getListObject(input1);
		
		//list append instruction
		if( getOpcode().equals("append") ) {
			//copy on write and append unnamed argument
			Data dat2 = ec.getVariable(input2);
			LineageItem li = DMLScript.LINEAGE ? ec.getLineage().get(input2):null;
			ListObject tmp = lo.copy().add(dat2, li);
			//set output variable
			ec.setVariable(output.getName(), tmp);
		}
		//list remove instruction
		else if( getOpcode().equals("remove") ) {
			//copy on write and remove by position
			ScalarObject dat2 = ec.getScalarInput(input2);
			ListObject tmp1 = lo.copy();
			ListObject tmp2 = tmp1.remove((int)dat2.getLongValue()-1);
			
			//set output variables
			ec.setVariable(output.getName(), tmp1);
			ec.setVariable(output2.getName(), tmp2);
		}
		else {
			throw new DMLRuntimeException("Unsupported list operation: "+getOpcode());
		}
	}
}
