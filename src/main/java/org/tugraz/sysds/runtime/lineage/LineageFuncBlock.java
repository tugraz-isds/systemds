package org.tugraz.sysds.runtime.lineage;

import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.BasicProgramBlock;
import org.tugraz.sysds.runtime.controlprogram.ProgramBlock;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.instructions.Instruction;

public class LineageFuncBlock {
	private LineageMap _funcMap = new LineageMap(); //TODO: multiple and nested functions
	
	public void traceBasicProgramBlock (BasicProgramBlock bpb, ExecutionContext ec) {
		traceInstructions(bpb, ec);
	}
	
	public LineageMap getActiveMap() {
		return _funcMap;
	}
	
	public void traceInstructions (ProgramBlock pb, ExecutionContext ec) {
		if (!(pb instanceof BasicProgramBlock))
			throw new DMLRuntimeException("Only BasicProgramBlocks are expected here");
		BasicProgramBlock bpb = (BasicProgramBlock)pb;
		for (Instruction inst : bpb.getInstructions())
			_funcMap.trace(inst, ec);
	}
}
