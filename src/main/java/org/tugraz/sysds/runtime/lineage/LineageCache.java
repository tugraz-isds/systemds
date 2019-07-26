package org.tugraz.sysds.runtime.lineage;

import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.instructions.Instruction;
import org.tugraz.sysds.runtime.instructions.cp.ComputationCPInstruction;
import org.tugraz.sysds.runtime.instructions.cp.Data;

import java.util.HashMap;
import java.util.Map;

public class LineageCache {
	private static final Map<LineageItem, Data> _cache = new HashMap<>();
	
	public static void put(LineageItem key, Data value) {
		_cache.put(key, value);
	}
	
	public static boolean probe(LineageItem key) {
		return _cache.containsKey(key);
	}
	
	public static Data get(LineageItem key) {
		return _cache.get(key);
	}
	
	public static boolean reuse(Instruction inst, ExecutionContext ec) {
		if (inst instanceof ComputationCPInstruction) {
			boolean reused = true;
			LineageItem[] items = ((ComputationCPInstruction) inst).getLineageItems();
			for (LineageItem item : items) {
				if (LineageCache.probe(item)) {
					Data d = LineageCache.get(item);
					ec.setVariable(((ComputationCPInstruction) inst).output.getName(), d);
				} else
					reused = false;
			}
			return reused && items.length > 0;
		} else {
			return false;
		}
	}
}
