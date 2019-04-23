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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.parfor.util.IDSequence;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;

public class LineageItem {
	private static IDSequence _idSeq = new IDSequence();
	
	private final long _id;
	private final String _opcode;
	private final CPOperand _variable;
	private final String _name;
	private final String _representation;
	private List<LineageItem> _inputs;
	private List<LineageItem> _outputs;
	private boolean _visited = false;
	
	public LineageItem(CPOperand variable, String representation) {
		this(variable, representation, "");
	}
	
	public LineageItem(CPOperand variable, List<LineageItem> inputs, String opcode) {
		this(variable, "", inputs, opcode);
	}
	
	public LineageItem(CPOperand variable, String representation, String opcode) {
		this(variable, representation, null, opcode);
	}
	
	public LineageItem(CPOperand variable, String representation, List<LineageItem> inputs, String opcode) {
		if (variable == null)
			throw new DMLRuntimeException("Parameter CPOperand variable is null.");
		
		_id = _idSeq.getNextID();
		_variable = variable;
		_opcode = opcode;
		_name = variable.getName();
		_representation = representation;
		
		if (inputs != null) {
			_inputs = new ArrayList<>(inputs);
			for (LineageItem li : _inputs)
				li._outputs.add(this);
		} else
			_inputs = null;
		_outputs = new ArrayList<>();
	}
	
	public LineageItem(String name) {
		_id = _idSeq.getNextID();
		_variable = null;
		_opcode = "";
		_name = name;
		_representation = name;
		_inputs = null;
		_outputs = new ArrayList<>();
	}
	
	public CPOperand getVariable() {
		return _variable;
	}
	
	public List<LineageItem> getInputs() {
		return _inputs;
	}
	
	public void removeAllInputs() {
		_inputs = new ArrayList<>();
	}
	
	public List<LineageItem> getOutputs() {
		return _outputs;
	}
	
	public String getName() {
		return _name;
	}
	
	public String getRepresentation() {
		return _representation;
	}
	
	public boolean isVisited() {
		return _visited;
	}
	
	public void setVisited() {
		setVisited(true);
	}
	
	public void setVisited(boolean flag) {
		_visited = flag;
	}
	
	public long getId() {
		return _id;
	}
	
	public String getOpcode() {
		return _opcode;
	}
	
	public String explain() {
		StringBuilder sb = new StringBuilder();
		sb.append("(").append(_id).append(") ");
		sb.append("(").append(getLineageItemType()).append(") ");
		
		if (isLeaf()) {
			sb.append(getRepresentation()).append(" ");
		} else {
			sb.append(_opcode).append(" ");
			String ids = _inputs.stream()
					.map(i -> String.format("(%d)", i._id))
					.collect(Collectors.joining(" "));
			sb.append(ids);
		}
		return sb.toString().trim();
	}
	
	private boolean isLeaf() {
		if (_inputs == null)
			return true;
		return _inputs.isEmpty();
	}
	
	private boolean isInstruction() {
		return !_opcode.isEmpty();
	}
	
	
	private String getLineageItemType() {
		StringBuilder sb = new StringBuilder();
		if (isLeaf())
			sb.append("L");
		else
			sb.append("N");
		
		if (isInstruction())
			sb.append("I");
		else
			sb.append("L");
		return sb.toString();
	}
	
	public LineageItem resetVisitStatus() {
		if (!isVisited())
			return this;
		if (_inputs != null && !_inputs.isEmpty())
			for (LineageItem li : getInputs())
				li.resetVisitStatus();
		setVisited(false);
		return this;
	}
	
	public static void resetVisitStatus(List<LineageItem> lis) {
		if (lis != null)
			for (LineageItem liRoot : lis)
				liRoot.resetVisitStatus();
	}
	
	public static void resetIDSequence() {
		_idSeq.reset(-1);
	}
}
