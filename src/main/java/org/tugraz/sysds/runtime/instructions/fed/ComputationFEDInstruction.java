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

package org.tugraz.sysds.runtime.instructions.fed;

import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.runtime.controlprogram.caching.CacheableData;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;
import org.tugraz.sysds.runtime.lineage.LineageItem;
import org.tugraz.sysds.runtime.lineage.LineageItemUtils;
import org.tugraz.sysds.runtime.lineage.LineageTraceable;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.operators.Operator;

public abstract class ComputationFEDInstruction extends FEDInstruction implements LineageTraceable {
	
	public final CPOperand output;
	public final CPOperand input1, input2, input3;
	
	protected ComputationFEDInstruction(FEDType type, Operator op, CPOperand in1, CPOperand in2, CPOperand out,
			String opcode,
			String istr) {
		super(type, op, opcode, istr);
		input1 = in1;
		input2 = in2;
		input3 = null;
		output = out;
	}
	
	protected ComputationFEDInstruction(FEDType type, Operator op, CPOperand in1, CPOperand in2, CPOperand in3,
			CPOperand out,
			String opcode, String istr) {
		super(type, op, opcode, istr);
		input1 = in1;
		input2 = in2;
		input3 = in3;
		output = out;
	}
	
	public String getOutputVariableName() {
		return output.getName();
	}
	
	protected boolean checkGuardedRepresentationChange(MatrixBlock in1, MatrixBlock out) {
		return checkGuardedRepresentationChange(in1, null, out);
	}
	
	protected boolean checkGuardedRepresentationChange(MatrixBlock in1, MatrixBlock in2, MatrixBlock out) {
		if( DMLScript.getGlobalExecMode() == ExecMode.SINGLE_NODE
				&& !CacheableData.isCachingActive() )
			return true;
		double memIn1 = (in1 != null) ? in1.getInMemorySize() : 0;
		double memIn2 = (in2 != null) ? in2.getInMemorySize() : 0;
		double memReq = out.isInSparseFormat() ?
				MatrixBlock.estimateSizeDenseInMemory(out.getNumRows(), out.getNumColumns()) :
				MatrixBlock.estimateSizeSparseInMemory(out.getNumRows(), out.getNumColumns(), out.getSparsity());
		//guarded if mem requirements smaller than input sizes
		return (memReq < memIn1 + memIn2
				+ OptimizerUtils.SAFE_REP_CHANGE_THRES); //8MB
	}
	
	@Override
	public LineageItem[] getLineageItems(ExecutionContext ec) {
		return new LineageItem[]{new LineageItem(output.getName(),
				getOpcode(), LineageItemUtils.getLineage(ec, input1, input2, input3))};
	}
}
