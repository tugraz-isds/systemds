package org.tugraz.sysds.runtime.lineage;

import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.*;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.instructions.Instruction;

import java.util.HashMap;
import java.util.Map;

public class LineageDedupBlock {
	private Map<Integer, LineageMap> _distinctPaths = new HashMap<>();
	private Integer _activePath = null;
	
	public LineageMap getActiveMap() {
		if (_activePath == null || !_distinctPaths.containsKey(_activePath))
			throw new DMLRuntimeException("Active path in LineageDedupBlock could not be found.");
		return _distinctPaths.get(_activePath);
	}
	
	public LineageMap getMap(int path) {
		if (!_distinctPaths.containsKey(path))
			throw new DMLRuntimeException("Given path in LineageDedupBlock could not be found.");
		return _distinctPaths.get(path);
	}
	
	void addBranch(IfProgramBlock ipb, ExecutionContext ec) {
		Integer size = _distinctPaths.size();

//		if (ipb.getChildBlocksIfBody() != null) {
//			if (ipb.getChildBlocksIfBody().size() == 1)
//				items.add(computeLineage(ipb.getChildBlocksIfBody().get(0), ec));
//			else
//				throw new DMLRuntimeException("Deduplication does not support multiple program blocks in a branch!");
//		}
//		if (ipb.getChildBlocksElseBody() != null) {
//			if (ipb.getChildBlocksElseBody().size() == 1)
//				items.add(computeLineage(ipb.getChildBlocksElseBody().get(0), ec));
//			else
//				throw new DMLRuntimeException("Deduplication does not support multiple program blocks in a branch!");
//		}
	}
	
	void addLineage(ProgramBlock pb, ExecutionContext ec) {
		//TODO bnyra: This is very bad!!!
		if (pb instanceof WhileProgramBlock ||
				pb instanceof FunctionProgramBlock ||
				pb instanceof ForProgramBlock ||
				pb instanceof IfProgramBlock)
			throw new DMLRuntimeException("Function only supports ProgramBlock");
		
		if (_distinctPaths.size() == 0)
			_distinctPaths.put(-1, new LineageMap());
		
		for (Instruction inst : pb.getInstructions()) {
			for (Map.Entry<Integer, LineageMap> entry : _distinctPaths.entrySet()) {
				_activePath = entry.getKey();
				entry.getValue().trace(inst, ec);
			}
		}
		_activePath = null;
	}
}
