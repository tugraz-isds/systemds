/*
 * Modifications Copyright 2019 Graz University of Technology
 *
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

package org.tugraz.sysds.runtime.instructions.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.tugraz.sysds.common.Types.DataType;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.tugraz.sysds.runtime.instructions.InstructionUtils;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.data.MatrixIndexes;
import org.tugraz.sysds.runtime.matrix.operators.Operator;
import org.tugraz.sysds.runtime.matrix.operators.UnaryOperator;

public class UnaryMatrixSPInstruction extends UnarySPInstruction {

	protected UnaryMatrixSPInstruction(Operator op, CPOperand in, CPOperand out, String opcode, String instr) {
		super(SPType.Unary, op, in, out, opcode, instr);
	}
	
	public static UnarySPInstruction parseInstruction ( String str ) {
		CPOperand in = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
		CPOperand out = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
		String opcode = parseUnaryInstruction(str, in, out);
		return new UnaryMatrixSPInstruction(
			InstructionUtils.parseUnaryOperator(opcode), in, out, opcode, str);
	}

	@Override 
	public void processInstruction(ExecutionContext ec) {
		SparkExecutionContext sec = (SparkExecutionContext)ec;
		
		//get input
		JavaPairRDD<MatrixIndexes,MatrixBlock> in = sec.getBinaryMatrixBlockRDDHandleForVariable( input1.getName() );
		
		//execute unary builtin operation
		UnaryOperator uop = (UnaryOperator) _optr;
		JavaPairRDD<MatrixIndexes,MatrixBlock> out = in.mapValues(new RDDMatrixBuiltinUnaryOp(uop));
		
		//set output RDD
		updateUnaryOutputDataCharacteristics(sec);
		sec.setRDDHandleForVariable(output.getName(), out);	
		sec.addLineageRDD(output.getName(), input1.getName());
	}

	private static class RDDMatrixBuiltinUnaryOp implements Function<MatrixBlock,MatrixBlock> 
	{
		private static final long serialVersionUID = -3128192099832877491L;
		
		private UnaryOperator _op = null;
		
		public RDDMatrixBuiltinUnaryOp(UnaryOperator u_op) {
			_op = u_op;
		}

		@Override
		public MatrixBlock call(MatrixBlock arg0) throws Exception {
			return arg0.unaryOperations(_op, new MatrixBlock());
		}
	}
}

