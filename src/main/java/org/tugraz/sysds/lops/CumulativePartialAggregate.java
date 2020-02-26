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

package org.tugraz.sysds.lops;

import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.runtime.instructions.InstructionUtils;
import org.tugraz.sysds.common.Types.AggOp;
import org.tugraz.sysds.common.Types.DataType;
import org.tugraz.sysds.common.Types.ValueType;

public class CumulativePartialAggregate extends Lop 
{
	private AggOp _op;
	
	public CumulativePartialAggregate(Lop input, DataType dt, ValueType vt, AggOp op, ExecType et) {
		super(Lop.Type.CumulativePartialAggregate, dt, vt);
		
		//sanity check for supported aggregates
		if( !( op == AggOp.SUM || op == AggOp.PROD 
			|| op == AggOp.SUM_PROD
			|| op == AggOp.MIN || op == AggOp.MAX) )
		{
			throw new LopsException("Unsupported aggregate operation type: "+op);
		}
		_op = op;
		init(input, dt, vt, et);
	}
	
	private void init(Lop input, DataType dt, ValueType vt, ExecType et) {
		this.addInput(input);
		input.addOutput(this);
		lps.setProperties( inputs, et);
	}

	@Override
	public String toString() {
		return "CumulativePartialAggregate";
	}
	
	private String getOpcode() {
		switch( _op ) {
			case SUM:      return "ucumack+";
			case PROD:     return "ucumac*";
			case SUM_PROD: return "ucumac+*";
			case MIN:      return "ucumacmin";
			case MAX:      return "ucumacmax";
			default:       return null;
		}
	}
	
	@Override
	public String getInstructions(String input, String output) {
		return InstructionUtils.concatOperands(
			getExecType().name(),
			getOpcode(),
			getInputs().get(0).prepInputOperand(input),
			prepOutputOperand(output));
	}
}
