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

import org.tugraz.sysds.runtime.controlprogram.ForProgramBlock;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.instructions.Instruction;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;
import org.tugraz.sysds.runtime.lineage.LineageCacheConfig.ReuseCacheType;
import org.tugraz.sysds.runtime.util.ProgramConverter;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.StringTokenizer;

import static org.tugraz.sysds.utils.Explain.explain;

public class Lineage {
	private final LineageMap _map;
	private final Stack<LineageDedupBlock> _initDedupBlock = new Stack<>();
	private final Stack<LineageDedupBlock> _activeDedupBlock = new Stack<>();
	private final Map<ForProgramBlock, LineageDedupBlock> _dedupBlocks = new HashMap<>();
	
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
	
	public LineageItem getOrCreate(CPOperand variable) {
		return _initDedupBlock.empty() ?
			_map.getOrCreate(variable) :
			_initDedupBlock.peek().getActiveMap().getOrCreate(variable);
	}
	
	public boolean contains(CPOperand variable) {
		return _initDedupBlock.empty() ?
			_map.containsKey(variable.getName()) :
			_initDedupBlock.peek().getActiveMap().containsKey(variable.getName());
	}
	
	public LineageItem get(String varName) {
		return _map.get(varName);
	}
	
	public void set(String varName, LineageItem li) {
		_map.set(varName, li);
	}
	
	public void setLiteral(String varName, LineageItem li) {
		_map.setLiteral(varName, li);
	}
	
	public LineageItem get(CPOperand variable) {
		return _initDedupBlock.empty() ?
			_map.get(variable) :
			_initDedupBlock.peek().getActiveMap().get(variable);
	}
	
	public void pushInitDedupBlock(LineageDedupBlock ldb) {
		_initDedupBlock.push(ldb);
	}
	
	public LineageDedupBlock popInitDedupBlock() {
		return _initDedupBlock.pop();
	}
	
	public void computeDedupBlock(ForProgramBlock fpb, ExecutionContext ec) {
		if (!_dedupBlocks.containsKey(fpb))
			_dedupBlocks.put(fpb, LineageDedupUtils.computeDedupBlock(fpb, ec));
		_activeDedupBlock.push(_dedupBlocks.get(fpb));
	}
	
	public void clearDedupBlock() {
		_activeDedupBlock.pop();
	}
	
	public String serialize() {
		StringBuilder sb = new StringBuilder();
		int count = 0;
		for (Map.Entry<String, LineageItem> e : _map.getTraces().entrySet()) {
			if (count != 0)
				sb.append (ProgramConverter.ELEMENT_DELIM);
			
			sb.append(e.getKey());
			sb.append(ProgramConverter.COMPONENTS_DELIM);
			sb.append(explain(e.getValue()));
			count++;
		}

		sb.append(ProgramConverter.ELEMENT_DELIM2);
		
		count = 0;
		for (Map.Entry<String, LineageItem> e : _map.getLiterals().entrySet()) {
			if (count != 0)
				sb.append (ProgramConverter.ELEMENT_DELIM);
			
			sb.append(e.getKey());
			sb.append(ProgramConverter.COMPONENTS_DELIM);
			sb.append(explain(e.getValue()));
			count++;
		}
		
		return sb.toString();
	}
	
	public static Lineage deserialize(String lingStr) {
		StringTokenizer tokenizer = new StringTokenizer(lingStr, ProgramConverter.ELEMENT_DELIM2);
		Lineage ling = new Lineage();
		
		String traces = tokenizer.nextToken().trim();
		String literals = tokenizer.nextToken().trim();
		
		tokenizer = new StringTokenizer(traces, ProgramConverter.ELEMENT_DELIM);
		while (tokenizer.hasMoreTokens()) {
			String tmp = tokenizer.nextToken().trim();
			String name = tmp.substring(0, tmp.indexOf(ProgramConverter.COMPONENTS_DELIM));
			tmp = tmp.substring(tmp.indexOf(ProgramConverter.COMPONENTS_DELIM)+1);
			LineageItem item = LineageParser.parseLineageTrace(tmp);
			ling.set(name, item);
		}
		
		tokenizer = new StringTokenizer(literals, ProgramConverter.ELEMENT_DELIM);
		while (tokenizer.hasMoreTokens()) {
			String tmp = tokenizer.nextToken().trim();
			String name = tmp.substring(0, tmp.indexOf(ProgramConverter.COMPONENTS_DELIM));
			tmp = tmp.substring(tmp.indexOf(ProgramConverter.COMPONENTS_DELIM)+1);
			LineageItem item = LineageParser.parseLineageTrace(tmp);
			ling.setLiteral(name, item);
		}
		return ling;
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
