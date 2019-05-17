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
	private Integer _branches = 0;
	
	public LineageMap getActiveMap() {
		if (_activePath == null || !_distinctPaths.containsKey(_activePath))
			throw new DMLRuntimeException("Active path in LineageDedupBlock could not be found.");
		return _distinctPaths.get(_activePath);
	}
	
	public LineageMap getMap(Integer path) {
		if (!_distinctPaths.containsKey(path))
			throw new DMLRuntimeException("Given path in LineageDedupBlock could not be found.");
		return _distinctPaths.get(path);
	}
	
	public void traceIfProgramBlock(IfProgramBlock ipb, ExecutionContext ec) {
		addPathsForBranch();
		traceIfBodyInstructions(ipb, ec);
		traceElseBodyInstructions(ipb, ec);
	}
	
	public void traceProgramBlock(ProgramBlock pb, ExecutionContext ec) {
		if (_distinctPaths.size() == 0)
			_distinctPaths.put(0, new LineageMap());
		
		traceInstructions(pb, ec);
	}
	
	private void traceInstructions(ProgramBlock pb, ExecutionContext ec) {
		for (Map.Entry<Integer, LineageMap> entry : _distinctPaths.entrySet()) {
			for (Instruction inst : pb.getInstructions()) {
				_activePath = entry.getKey();
				entry.getValue().trace(inst, ec);
			}
		}
		_activePath = null;
	}
	
	private void traceIfBodyInstructions(IfProgramBlock ipb, ExecutionContext ec) {
		// Add IfBody instructions to lower half of LineageMaps
		if (ipb.getChildBlocksIfBody() != null && ipb.getChildBlocksIfBody().size() == 1) {
			for (Map.Entry<Integer, LineageMap> entry : _distinctPaths.entrySet()) {
				if (entry.getKey() < _branches) {
					for (Instruction inst : ipb.getChildBlocksIfBody().get(0).getInstructions()) {
						_activePath = entry.getKey();
						entry.getValue().trace(inst, ec);
					}
				} else
					break;
			}
		}
	}
	
	private void traceElseBodyInstructions(IfProgramBlock ipb, ExecutionContext ec) {
		// Add ElseBody instructions to upper half of LineageMaps
		if (ipb.getChildBlocksElseBody() != null && ipb.getChildBlocksElseBody().size() == 1) {
			for (Map.Entry<Integer, LineageMap> entry : _distinctPaths.entrySet()) {
				if (entry.getKey() >= _branches) {
					for (Instruction inst : ipb.getChildBlocksElseBody().get(0).getInstructions()) {
						_activePath = entry.getKey();
						entry.getValue().trace(inst, ec);
					}
				}
			}
		}
		_activePath = null;
	}
	
	private void addPathsForBranch() {
		if (_distinctPaths.size() == 0) {
			_distinctPaths.put(0, new LineageMap());
			_distinctPaths.put(1, new LineageMap());
		} else {
			Map<Integer, LineageMap> elseBranches = new HashMap<>();
			for (Map.Entry<Integer, LineageMap> entry : _distinctPaths.entrySet()) {
				Integer pathIndex = entry.getKey() | 1 << _branches;
				elseBranches.put(pathIndex, new LineageMap(entry.getValue()));
			}
			_distinctPaths.putAll(elseBranches);
		}
		_branches++;
	}
	
}
