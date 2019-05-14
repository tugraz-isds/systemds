package org.tugraz.sysds.runtime.lineage;

import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.*;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;

public class LineageDedupUtils {

	static LineageDedupBlock computeDistinctPaths(ForProgramBlock fpb, ExecutionContext ec) {
		LineageDedupBlock ldb = new LineageDedupBlock();
		Lineage.setInitDedupBlock(ldb);
		
		for (ProgramBlock pb : fpb.getChildBlocks()) {
			//TODO bnyra: This is very bad!!!
			if (pb instanceof WhileProgramBlock || pb instanceof FunctionProgramBlock || pb instanceof ForProgramBlock)
				throw new DMLRuntimeException("Deduplication is not supported for nested while for or function calls!");

			if (pb instanceof IfProgramBlock)
			{
				assert false;
				System.out.println("Skip if program block");
				// TODO bnyra: implement addBranch stuff
//				ldi.addBranch((IfProgramBlock) pb, ec);
			}
			else
				ldb.addLineage(pb, ec);
		}
		
		Lineage.setInitDedupBlock(null);
		return ldb;
	}
}
