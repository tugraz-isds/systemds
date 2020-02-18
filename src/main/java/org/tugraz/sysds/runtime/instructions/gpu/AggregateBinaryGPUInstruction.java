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
package org.tugraz.sysds.runtime.instructions.gpu;

import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.caching.MatrixObject;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.functionobjects.Multiply;
import org.tugraz.sysds.runtime.functionobjects.Plus;
import org.tugraz.sysds.runtime.functionobjects.SwapIndex;
import org.tugraz.sysds.runtime.instructions.InstructionUtils;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;
import org.tugraz.sysds.runtime.matrix.data.LibMatrixCUDA;
import org.tugraz.sysds.runtime.matrix.data.LibMatrixCuMatMult;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.tugraz.sysds.runtime.matrix.operators.AggregateOperator;
import org.tugraz.sysds.runtime.matrix.operators.Operator;
import org.tugraz.sysds.runtime.matrix.operators.ReorgOperator;
import org.tugraz.sysds.utils.GPUStatistics;

public class AggregateBinaryGPUInstruction extends GPUInstruction {
	private CPOperand _input1 = null;
	private CPOperand _input2 = null;
	private CPOperand _output = null;
	private boolean _isLeftTransposed;
	private boolean _isRightTransposed;

	private AggregateBinaryGPUInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, String opcode,
			String istr, boolean leftTranspose, boolean rightTranspose) {
		super(op, opcode, istr);
		_gputype = GPUINSTRUCTION_TYPE.AggregateBinary;
		_input1 = in1;
		_input2 = in2;
		_output = out;
		_isLeftTransposed = leftTranspose;
		_isRightTransposed = rightTranspose;
	}

	public static AggregateBinaryGPUInstruction parseInstruction( String str ) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		if ( !opcode.equalsIgnoreCase("ba+*"))
 			throw new DMLRuntimeException("AggregateBinaryInstruction.parseInstruction():: Unknown opcode " + opcode);
		InstructionUtils.checkNumFields( parts, 5 );
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand in2 = new CPOperand(parts[2]);
		CPOperand out = new CPOperand(parts[3]);
		boolean isLeftTransposed = Boolean.parseBoolean(parts[4]);
		boolean isRightTransposed = Boolean.parseBoolean(parts[5]);
		AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
		AggregateBinaryOperator aggbin = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg, 1);
		return new AggregateBinaryGPUInstruction(aggbin, in1, in2, out, opcode, str, isLeftTransposed, isRightTransposed);	
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		GPUStatistics.incrementNoOfExecutedGPUInst();
		AggregateBinaryOperator op = (AggregateBinaryOperator) _optr;
		if( !(op.binaryFn instanceof Multiply && op.aggOp.increOp.fn instanceof Plus) )
			throw new DMLRuntimeException("Unsupported binary aggregate operation: ("+op.binaryFn+", "+op.aggOp+").");
		MatrixObject m1 = getMatrixInputForGPUInstruction(ec, _input1.getName());
		MatrixObject m2 = getMatrixInputForGPUInstruction(ec, _input2.getName());
		//compute matrix multiplication
		int rlen = (int) (_isLeftTransposed ? m1.getNumColumns() : m1.getNumRows());
		int clen = (int) (_isRightTransposed ? m2.getNumRows() : m2.getNumColumns());
		ec.setMetaData(_output.getName(), rlen, clen);
		LibMatrixCuMatMult.matmult(ec, ec.getGPUContext(0), getExtendedOpcode(), m1, m2, _output.getName(), _isLeftTransposed, _isRightTransposed);
		//release inputs/outputs
		ec.releaseMatrixInputForGPUInstruction(_input1.getName());
		ec.releaseMatrixInputForGPUInstruction(_input2.getName());
		ec.releaseMatrixOutputForGPUInstruction(_output.getName());
	}

	@SuppressWarnings("unused")
	private static MatrixBlock transpose(MatrixBlock m1) {
		ReorgOperator r_op = new ReorgOperator(SwapIndex.getSwapIndexFnObject(), 1);
		return m1.reorgOperations(r_op, new MatrixBlock(), 0, 0, 0);
	}

	@SuppressWarnings("unused")
	private static boolean isSparse(ExecutionContext ec, String var) {
		MatrixObject mo = ec.getMatrixObject(var);
		return LibMatrixCUDA.isInSparseFormat(ec.getGPUContext(0), mo);
	}
}
