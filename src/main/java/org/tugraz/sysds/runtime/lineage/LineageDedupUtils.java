package org.tugraz.sysds.runtime.lineage;

import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.*;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;

public class LineageDedupUtils {
	
	public static LineageDedupBlock computeDistinctPaths(ForProgramBlock fpb, ExecutionContext ec) {
		LineageDedupBlock ldb = new LineageDedupBlock();
		Lineage.pushInitDedupBlock(ldb);
		
		for (ProgramBlock pb : fpb.getChildBlocks()) {
			if (pb instanceof IfProgramBlock) {
				ldb.traceIfProgramBlock((IfProgramBlock) pb, ec);
			} else if (pb instanceof BasicProgramBlock) {
				ldb.traceBasicProgramBlock((BasicProgramBlock) pb, ec);
			} else if (pb instanceof ForProgramBlock) {
				ldb.splitBlocks();
			} else
				throw new DMLRuntimeException("Only BasicProgramBlocks or IfProgramBlocks are allowed inside a LineageDedupBlock.");
		}
		
		Lineage.popInitDedupBlock();
		return ldb;
	}
}
