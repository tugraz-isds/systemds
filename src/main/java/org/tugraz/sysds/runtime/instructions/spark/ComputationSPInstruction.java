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

package org.tugraz.sysds.runtime.instructions.spark;

import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.tugraz.sysds.runtime.functionobjects.IndexFunction;
import org.tugraz.sysds.runtime.functionobjects.ReduceAll;
import org.tugraz.sysds.runtime.functionobjects.ReduceCol;
import org.tugraz.sysds.runtime.functionobjects.ReduceRow;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;
import org.tugraz.sysds.runtime.lineage.Lineage;
import org.tugraz.sysds.runtime.lineage.LineageItem;
import org.tugraz.sysds.runtime.lineage.LineageTraceable;
import org.tugraz.sysds.runtime.matrix.operators.Operator;
import org.tugraz.sysds.runtime.meta.MatrixCharacteristics;

import java.util.ArrayList;

public abstract class ComputationSPInstruction extends SPInstruction implements LineageTraceable {
	public CPOperand output;
	public CPOperand input1, input2, input3;

	protected ComputationSPInstruction(SPType type, Operator op, CPOperand in1, CPOperand in2, CPOperand out, String opcode, String istr) {
		super(type, op, opcode, istr);
		input1 = in1;
		input2 = in2;
		input3 = null;
		output = out;
	}

	protected ComputationSPInstruction(SPType type, Operator op, CPOperand in1, CPOperand in2, CPOperand in3, CPOperand out, String opcode, String istr) {
		super(type, op, opcode, istr);
		input1 = in1;
		input2 = in2;
		input3 = in3;
		output = out;
	}

	public String getOutputVariableName() {
		return output.getName();
	}

	protected void updateUnaryOutputMatrixCharacteristics(SparkExecutionContext sec) {
		updateUnaryOutputMatrixCharacteristics(sec, input1.getName(), output.getName());
	}

	protected void updateUnaryOutputMatrixCharacteristics(SparkExecutionContext sec, String nameIn, String nameOut) {
		MatrixCharacteristics mc1 = sec.getMatrixCharacteristics(nameIn);
		MatrixCharacteristics mcOut = sec.getMatrixCharacteristics(nameOut);
		if(!mcOut.dimsKnown()) {
			if(!mc1.dimsKnown())
				throw new DMLRuntimeException("The output dimensions are not specified and cannot be inferred from input:" + mc1.toString() + " " + mcOut.toString());
			else
				mcOut.set(mc1.getRows(), mc1.getCols(), mc1.getRowsPerBlock(), mc1.getColsPerBlock());
		}
	}

	protected void updateBinaryOutputMatrixCharacteristics(SparkExecutionContext sec) {
		MatrixCharacteristics mcIn1 = sec.getMatrixCharacteristics(input1.getName());
		MatrixCharacteristics mcIn2 = sec.getMatrixCharacteristics(input2.getName());
		MatrixCharacteristics mcOut = sec.getMatrixCharacteristics(output.getName());
		boolean outer = (mcIn1.getRows()>1 && mcIn1.getCols()==1 && mcIn2.getRows()==1 && mcIn2.getCols()>1);
		
		if(!mcOut.dimsKnown()) {
			if(!mcIn1.dimsKnown())
				throw new DMLRuntimeException("The output dimensions are not specified and cannot be inferred from input:" + mcIn1.toString() + " " + mcIn2.toString() + " " + mcOut.toString());
			else if(outer)
				sec.getMatrixCharacteristics(output.getName()).set(mcIn1.getRows(), mcIn2.getCols(), mcIn1.getRowsPerBlock(), mcIn2.getColsPerBlock());
			else
				sec.getMatrixCharacteristics(output.getName()).set(mcIn1.getRows(), mcIn1.getCols(), mcIn1.getRowsPerBlock(), mcIn1.getRowsPerBlock());
		}
	}
	
	protected void updateUnaryAggOutputMatrixCharacteristics(SparkExecutionContext sec, IndexFunction ixFn) {
		MatrixCharacteristics mc1 = sec.getMatrixCharacteristics(input1.getName());
		MatrixCharacteristics mcOut = sec.getMatrixCharacteristics(output.getName());
		if( mcOut.dimsKnown() )
			return;
		
		if(!mc1.dimsKnown()) {
			throw new DMLRuntimeException("The output dimensions are not specified and "
				+ "cannot be inferred from input:" + mc1.toString() + " " + mcOut.toString());
		}
		else {
			//infer statistics from input based on operator
			if( ixFn instanceof ReduceAll )
				mcOut.set(1, 1, mc1.getRowsPerBlock(), mc1.getColsPerBlock());
			else if( ixFn instanceof ReduceCol )
				mcOut.set(mc1.getRows(), 1, mc1.getRowsPerBlock(), mc1.getColsPerBlock());
			else if( ixFn instanceof ReduceRow )
				mcOut.set(1, mc1.getCols(), mc1.getRowsPerBlock(), mc1.getColsPerBlock());
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
		return new LineageItem[]{new LineageItem(output.getName(),
				getOpcode(), lineages.toArray(new LineageItem[0]))};
	}
}
