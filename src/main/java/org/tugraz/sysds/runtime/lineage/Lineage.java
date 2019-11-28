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

package org.tugraz.sysds.runtime.lineage;

import org.tugraz.sysds.runtime.controlprogram.ProgramBlock;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.instructions.Instruction;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;
import org.tugraz.sysds.runtime.lineage.LineageCacheConfig.ReuseCacheType;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

public class Lineage {
	private final LineageMap _map;
	private final Stack<LineageDedupBlock> _initDedupBlock = new Stack<>();
	private final Stack<LineageDedupBlock> _activeDedupBlock = new Stack<>();
	private final Stack<LineageFuncBlock> _initFuncBlock = new Stack<>();
	private final Map<ProgramBlock, LineageDedupBlock> _dedupBlocks = new HashMap<>();
	
	public Lineage() {
		_map = new LineageMap();
	}
	
	public Lineage(Lineage that) {
		_map = new LineageMap(that._map);
	}
	
	public void trace(Instruction inst, ExecutionContext ec) {
		if (_activeDedupBlock.empty())
			_map.trace(inst, ec);
	}
	
	public void tracePath(int block, Long path) {
		LineageMap lm = _activeDedupBlock.peek().getMap(block, path);
		if (lm != null)
			_map.processDedupItem(lm, path);
	}
	
	public void traceFuncPath(int fn_block, Long fn_path, String var, ExecutionContext fn_ec) {
		LineageMap fn_lm = fn_ec.getLineage().getDedupBlock(fn_block, fn_path);
		if (fn_lm != null)
			_map.processFuncItem(fn_lm, fn_path, var);
		
	}
	
	public LineageMap getDedupBlock(int block, Long path) {
		return _activeDedupBlock.peek().getMap(block, path);
	}
	
	public LineageMap getLineageMap() {
		return _map;
	}
	
	public LineageItem getOrCreate(CPOperand variable) {
		//TODO: dedup inside functions and vice versa
		if (!_initDedupBlock.empty())
			return _initDedupBlock.peek().getActiveMap().getOrCreate(variable);
		else if (!_initFuncBlock.empty())
			return _initFuncBlock.peek().getActiveMap().getOrCreate(variable);
		else
			return _map.getOrCreate(variable);
	}
	
	public boolean contains(CPOperand variable) {
		if (!_initDedupBlock.empty())
			return _initDedupBlock.peek().getActiveMap().containsKey(variable.getName());
		else if (!_initFuncBlock.empty())
			return _initFuncBlock.peek().getActiveMap().containsKey(variable.getName());
		else
			return _map.containsKey(variable.getName());
	}
	
	public LineageItem get(String varName) {
		return _map.get(varName);
	}
	
	public void set(String varName, LineageItem li) {
		_map.set(varName, li);
	}
	
	public LineageItem get(CPOperand variable) {
		if (!_initDedupBlock.empty())
			return _initDedupBlock.peek().getActiveMap().get(variable);
		else if (!_initFuncBlock.empty())
			return _initFuncBlock.peek().getActiveMap().get(variable);
		else
			return _map.get(variable);
	}
	
	public void pushInitDedupBlock(LineageDedupBlock ldb) {
		_initDedupBlock.push(ldb);
	}

	public void pushInitFuncBlock(LineageFuncBlock lfb) {
		_initFuncBlock.push(lfb);
	}
	
	public LineageDedupBlock popInitDedupBlock() {
		return _initDedupBlock.pop();
	}
	
	public LineageFuncBlock popInitFuncBlock() {
		return _initFuncBlock.pop();
	}
	
	public void computeDedupBlock(ProgramBlock fpb, ExecutionContext ec) {
		if (!_dedupBlocks.containsKey(fpb))
			_dedupBlocks.put(fpb, LineageDedupUtils.computeDedupBlock(fpb, ec));
		_activeDedupBlock.push(_dedupBlocks.get(fpb));
	}
	
	public void clearDedupBlock() {
		_activeDedupBlock.pop();
	}
	
	public static void resetInternalState() {
		LineageItem.resetIDSequence();
		LineageCache.resetCache();
	}
	
	public static void setLinReusePartial() {
		LineageCacheConfig.setConfigTsmmCbind(ReuseCacheType.REUSE_PARTIAL);
	}

	public static void setLinReuseFull() {
		LineageCacheConfig.setConfigTsmmCbind(ReuseCacheType.REUSE_FULL);
	}
	
	public static void setLinReuseFullAndPartial() {
		LineageCacheConfig.setConfigTsmmCbind(ReuseCacheType.REUSE_HYBRID);
	}

	public static void setLinReuseNone() {
		LineageCacheConfig.setConfigTsmmCbind(ReuseCacheType.NONE);
	}
}
