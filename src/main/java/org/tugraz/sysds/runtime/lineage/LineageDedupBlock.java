package org.tugraz.sysds.runtime.lineage;

import org.tugraz.sysds.runtime.controlprogram.*;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;

import java.util.ArrayList;

public class LineageDedupBlock {
	private ArrayList<Object> _blocks = new ArrayList<>();
	
	public LineageDedupBlock() {
		_blocks.add(new DistinctPathBlock());
	}
	
	public LineageMap getMap(int block, Long path) {
		return ((DistinctPathBlock) _blocks.get(block)).getMap(path);
	}
	
	public LineageMap getActiveMap() {
		return ((DistinctPathBlock) _blocks.get(_blocks.size() - 1)).getActiveMap();
	}
	
	public void traceIfProgramBlock(IfProgramBlock ipb, ExecutionContext ec) {
		((DistinctPathBlock) _blocks.get(_blocks.size() - 1)).traceIfProgramBlock(ipb, ec);
	}
	
	public void traceBasicProgramBlock(BasicProgramBlock bpb, ExecutionContext ec) {
		((DistinctPathBlock) _blocks.get(_blocks.size() - 1)).traceBasicProgramBlock(bpb, ec);
	}
	
	public void splitBlocks() {
		_blocks.add(new DistinctPathBlock());
	}
}
