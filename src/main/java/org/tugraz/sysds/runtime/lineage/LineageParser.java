package org.tugraz.sysds.runtime.lineage;


import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.instructions.Instruction;
import org.tugraz.sysds.runtime.instructions.InstructionParser;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;

import java.util.HashMap;
import java.util.Map;

public class LineageParser {
	
	public static LineageItem parseLineage(String str) {
		LineageTokenizer tokenizer = new LineageTokenizer();
		tokenizer.add("\\(");
		tokenizer.add("[0-9]+", "id");
		tokenizer.add("\\) \\(");
		tokenizer.add("L|C|I", "type");
		tokenizer.add("\\) ");
		tokenizer.add(".+", "representation");
		
		LineageItem li = null;
		Map<Long, LineageItem> map = new HashMap<>();
		
		for (String line : str.split("\\r?\\n")) {
			if (line.trim().isEmpty())
				continue;
			
			li = null;
			Map<String, String> tokens = tokenizer.tokenize(line);
			Long id = new Long(tokens.get("id"));
			LineageItem.LineageItemType type = LineageItemUtils.getType(tokens.get("type"));
			
			switch (type) {
				case Creation:
					Instruction inst = InstructionParser.parseSingleInstruction(tokens.get("representation"));
					if (!(inst instanceof LineageTraceable))
						throw new DMLRuntimeException("Unknown Instruction (" + inst.getOpcode() + ") traced.");
					li = new LineageItem(id, ((LineageTraceable) inst).getLineageItem());
					break;
				case Literal:
					CPOperand variable = new CPOperand(tokens.get("representation"));
					li = new LineageItem(id, variable, tokens.get("representation"));
					break;
				case Instruction:
					System.out.println("Inner instruction!");
					break;
				default:
					throw new DMLRuntimeException("Unknown LineageItemType given!");
			}
			map.put(id, li);
		}
		return li;
	}
}