package org.tugraz.sysds.runtime.lineage;

import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.instructions.Instruction;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;
import org.tugraz.sysds.runtime.instructions.cp.VariableCPInstruction;
import org.tugraz.sysds.utils.Explain;

import java.util.HashMap;
import java.util.Map;

public class LineageMap {
	
	private Map<String, LineageItem> _traces = new HashMap<>();
	private Map<String, LineageItem> _literals = new HashMap<>();
	
	public LineageMap() {
	}
	
	public LineageMap(LineageMap other) {
		for (Map.Entry<String, LineageItem> entry : other._traces.entrySet()) {
			this._traces.put(entry.getKey(), new LineageItem(entry.getValue()));
		}
	}
	
	public void trace(Instruction inst, ExecutionContext ec) {
		if (!(inst instanceof LineageTraceable))
			throw new DMLRuntimeException("Unknown Instruction (" + inst.getOpcode() + ") traced.");
		
		LineageItem[] items = ((LineageTraceable) inst).getLineageItems();
		if (items == null || items.length < 1)
			trace(inst, ec, null);
		else
			for (LineageItem li : items)
				trace(inst, ec, li);
	}
	
	public void processDedupItem(LineageMap lm, Long path) {
		for (Map.Entry<String, LineageItem> entry : lm._traces.entrySet()) {
			if (_traces.containsKey(entry.getKey())) {
				addLineageItem(new LineageItem(entry.getKey(),
						path.toString(), LineageItem.dedupItemOpcode,
						new LineageItem[]{_traces.get(entry.getKey()), entry.getValue()}));
			}
		}
	}
	
	public LineageItem getOrCreate(CPOperand variable) {
		if (variable == null)
			return null;
		String varname = variable.getName();
		//handle literals (never in traces)
		if (variable.isLiteral()) {
			LineageItem ret = _literals.get(varname);
			if (ret == null)
				_literals.put(varname, ret = new LineageItem(
						varname, variable.getLineageLiteral()));
			return ret;
		}
		//handle variables
		LineageItem ret = _traces.get(variable.getName());
		return (ret != null) ? ret :
				new LineageItem(varname, variable.getLineageLiteral());
	}
	
	public LineageItem get(CPOperand variable) {
		if (variable == null)
			return null;
		return _traces.get(variable.getName());
	}
	
	public boolean contains(CPOperand variable) {
		return _traces.containsKey(variable.getName());
	}
	
	public boolean containsKey(String key) {
		return _traces.containsKey(key);
	}
	
	public void resetLineageMaps() {
		_traces.clear();
		_literals.clear();
	}
	
	private void trace(Instruction inst, ExecutionContext ec, LineageItem li) {
		if (inst instanceof VariableCPInstruction) {
			VariableCPInstruction vcp_inst = ((VariableCPInstruction) inst);
			
			switch (vcp_inst.getVariableOpcode()) {
				case AssignVariable:
				case CopyVariable: {
					processCopyLI(li);
					break;
				}
				case Read:
				case CreateVariable: {
					if (li != null)
						addLineageItem(li);
					break;
				}
				case RemoveVariable: {
					for (CPOperand input : vcp_inst.getInputs())
						removeLineageItem(input.getName());
					break;
				}
				case Write: {
					processWriteLI(vcp_inst, ec);
					break;
				}
				case MoveVariable: {
					processMoveLI(li);
					break;
				}
				default:
					throw new DMLRuntimeException("Unknown VariableCPInstruction (" + inst.getOpcode() + ") traced.");
			}
		} else
			addLineageItem(li);
		
	}
	
	
	private void processCopyLI(LineageItem li) {
		if (li.getInputs().length != 1)
			throw new DMLRuntimeException("AssignVariable and CopyVariable must have one input lineage item!");
		//add item or overwrite existing item
		_traces.put(li.getName(), li.getInputs()[0]);
	}
	
	private void removeLineageItem(String key) {
		//remove item if present
		_traces.remove(key);
	}
	
	private void addLineageItem(LineageItem li) {
		//add item or overwrite existing item
		_traces.put(li.getName(), li);
	}
	
	private void processWriteLI(VariableCPInstruction inst, ExecutionContext ec) {
		LineageItem li = get(inst.getInput1());
		String fName = ec.getScalarInput(inst.getInput2().getName(), Types.ValueType.STRING, inst.getInput2().isLiteral()).getStringValue();
		
		if (DMLScript.LINEAGE_DEDUP) {
			LineageItemUtils.writeTraceToHDFS(Explain.explain(li), fName + ".lineage.dedup");
			li = LineageItemUtils.rDecompress(li);
		}
		LineageItemUtils.writeTraceToHDFS(Explain.explain(li), fName + ".lineage");
	}
	
	private void processMoveLI(LineageItem li) {
		if (li.getName().equals("__pred"))
			removeLineageItem(li.getInputs()[0].getName());
		else
			addLineageItem(li);
	}
}
