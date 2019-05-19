package org.tugraz.sysds.runtime.lineage;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.instructions.Instruction;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;
import org.tugraz.sysds.runtime.instructions.cp.VariableCPInstruction;
import org.tugraz.sysds.runtime.io.IOUtilFunctions;
import org.tugraz.sysds.runtime.util.HDFSTool;
import org.tugraz.sysds.utils.Explain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.tugraz.sysds.runtime.lineage.LineageItemUtils.removeInputLinks;

public class LineageMap {
	
	private Map<String, LineageItem> _traces = new HashMap<>();
	
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
		
		LineageItem li = ((LineageTraceable) inst).getLineageItem();
		
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
	
	public void processDedupItem(LineageMap lm) {
		for (Map.Entry<String, LineageItem> entry : lm._traces.entrySet()) {
			if (_traces.containsKey(entry.getKey())) {
				ArrayList<LineageItem> list = new ArrayList<>();
				list.add(_traces.get(entry.getKey()));
				list.add(entry.getValue());
				addLineageItem(new LineageItem(entry.getKey(),  LineageItem.dedupItemOpcode, list));
			}
		}
	}
	
	public LineageItem getOrCreate(CPOperand variable) {
		if (variable == null)
			return null;
		if (!_traces.containsKey(variable.getName()))
			return new LineageItem(variable.getName(), variable.getLineageLiteral());
		return _traces.get(variable.getName());
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
	
	private void processCopyLI(LineageItem li) {
		if (li.getInputs().size() != 1)
			throw new DMLRuntimeException("AssignVariable and CopyVariable must have one input lineage item!");
		
		if (_traces.get(li.getName()) != null) {
			removeInputLinks(_traces.get(li.getName()));
			_traces.remove(li.getName());
		}
		_traces.put(li.getName(), li.getInputs().get(0));
	}
	
	private void removeLineageItem(String key) {
		if (!_traces.containsKey(key))
			return;
		
		removeInputLinks(_traces.get(key));
		_traces.remove(key);
	}
	
	private void addLineageItem(LineageItem li) {
		if (_traces.get(li.getName()) != null) {
			removeInputLinks(_traces.get(li.getName()));
			_traces.remove(li.getName());
		}
		_traces.put(li.getName(), li);
	}
	
	private void processWriteLI(VariableCPInstruction inst, ExecutionContext ec) {
		String desc = Explain.explain(get(inst.getInput1()));
		String fname = ec.getScalarInput(inst.getInput2().getName(), Types.ValueType.STRING, inst.getInput2().isLiteral()).getStringValue();
		fname += ".lineage";
		
		try {
			HDFSTool.writeStringToHDFS(desc, fname);
			FileSystem fs = IOUtilFunctions.getFileSystem(fname);
			if (fs instanceof LocalFileSystem) {
				Path path = new Path(fname);
				IOUtilFunctions.deleteCrcFilesFromLocalFileSystem(fs, path);
			}
			
		} catch (IOException e) {
			throw new DMLRuntimeException(e);
		}
	}
	
	private void processMoveLI(LineageItem li) {
		if (li.getName().equals("__pred"))
			removeLineageItem(li.getInputs().get(0).getName());
		else
			addLineageItem(li);
	}
}
