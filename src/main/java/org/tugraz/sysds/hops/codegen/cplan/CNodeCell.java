/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.tugraz.sysds.hops.codegen.cplan;

import java.util.ArrayList;

import org.tugraz.sysds.common.Types.AggOp;
import org.tugraz.sysds.hops.codegen.SpoofFusedOp.SpoofOutputDimsType;
import org.tugraz.sysds.runtime.codegen.SpoofCellwise.CellType;
import org.tugraz.sysds.runtime.util.UtilFunctions;

public class CNodeCell extends CNodeTpl 
{	
	private static final String TEMPLATE = 
			  "package codegen;\n"
			+ "import org.tugraz.sysds.runtime.codegen.LibSpoofPrimitives;\n"
			+ "import org.tugraz.sysds.runtime.codegen.SpoofCellwise;\n"
			+ "import org.tugraz.sysds.runtime.codegen.SpoofCellwise.AggOp;\n"
			+ "import org.tugraz.sysds.runtime.codegen.SpoofCellwise.CellType;\n"
			+ "import org.tugraz.sysds.runtime.codegen.SpoofOperator.SideInput;\n"
			+ "import org.apache.commons.math3.util.FastMath;\n"
			+ "\n"
			+ "public final class %TMP% extends SpoofCellwise {\n" 
			+ "  public %TMP%() {\n"
			+ "    super(CellType.%TYPE%, %SPARSE_SAFE%, %SEQ%, %AGG_OP%);\n"
			+ "  }\n"
			+ "  protected double genexec(double a, SideInput[] b, double[] scalars, int m, int n, long grix, int rix, int cix) { \n"
			+ "%BODY_dense%"
			+ "    return %OUT%;\n"
			+ "  }\n"
			+ "}\n";
	
	private CellType _type = null;
	private AggOp _aggOp = null;
	private boolean _sparseSafe = false;
	private boolean _containsSeq = true;
	private boolean _requiresCastdtm = false;
	private boolean _multipleConsumers = false;
	
	public CNodeCell(ArrayList<CNode> inputs, CNode output ) {
		super(inputs, output);
	}
	
	public void setMultipleConsumers(boolean flag) {
		_multipleConsumers = flag;
	}
	
	public boolean hasMultipleConsumers() {
		return _multipleConsumers;
	}
	
	public void setCellType(CellType type) {
		_type = type;
		_hash = 0;
	}
	
	public CellType getCellType() {
		return _type;
	}
	
	public void setAggOp(AggOp aggop) {
		_aggOp = aggop;
		_hash = 0;
	}
	
	public AggOp getAggOp() {
		return _aggOp;
	}
	
	public void setSparseSafe(boolean flag) {
		_sparseSafe = flag;
	}
	
	public boolean isSparseSafe() {
		return _sparseSafe;
	}
	
	public void setContainsSeq(boolean flag) {
		_containsSeq = flag;
	}
	
	public boolean containsSeq() {
		return _containsSeq;
	}
	
	public void setRequiresCastDtm(boolean flag) {
		_requiresCastdtm = flag;
		_hash = 0;
	}
	
	public boolean requiredCastDtm() {
		return _requiresCastdtm;
	}
	
	@Override
	public void renameInputs() {
		rRenameDataNode(_output, _inputs.get(0), "a");
		renameInputs(_inputs, 1);
	}
	
	@Override
	public String codegen(boolean sparse) {
		String tmp = TEMPLATE;
		
		//generate dense/sparse bodies
		String tmpDense = _output.codegen(false);
		_output.resetGenerated();

		tmp = tmp.replace("%TMP%", createVarname());
		tmp = tmp.replace("%BODY_dense%", tmpDense);
		
		//return last TMP
		tmp = tmp.replace("%OUT%", _output.getVarname());
		
		//replace meta data information
		tmp = tmp.replace("%TYPE%", getCellType().name());
		tmp = tmp.replace("%AGG_OP%", (_aggOp!=null) ? "AggOp."+_aggOp.name() : "null" );
		tmp = tmp.replace("%SPARSE_SAFE%", String.valueOf(isSparseSafe()));
		tmp = tmp.replace("%SEQ%", String.valueOf(containsSeq()));
		
		return tmp;
	}

	@Override
	public void setOutputDims() {
		
		
	}

	@Override
	public CNodeTpl clone() {
		CNodeCell tmp = new CNodeCell(_inputs, _output);
		tmp.setDataType(getDataType());
		tmp.setCellType(getCellType());
		tmp.setMultipleConsumers(hasMultipleConsumers());
		return tmp;
	}
	
	@Override
	public SpoofOutputDimsType getOutputDimType() {
		switch( _type ) {
			case NO_AGG: return SpoofOutputDimsType.INPUT_DIMS;
			case ROW_AGG: return SpoofOutputDimsType.ROW_DIMS;
			case COL_AGG: return SpoofOutputDimsType.COLUMN_DIMS_COLS;
			case FULL_AGG: return SpoofOutputDimsType.SCALAR;
			default:
				throw new RuntimeException("Unsupported cell type: "+_type.toString());
		}
	}

	@Override
	public int hashCode() {
		if( _hash == 0 ) {
			int h = super.hashCode();
			h = UtilFunctions.intHashCode(h, _type.hashCode());
			h = UtilFunctions.intHashCode(h, (_aggOp!=null) ? _aggOp.hashCode() : 0);
			h = UtilFunctions.intHashCode(h, Boolean.hashCode(_sparseSafe));
			h = UtilFunctions.intHashCode(h, Boolean.hashCode(_requiresCastdtm));
			//note: _multipleConsumers irrelevant for plan comparison
			_hash = h;
		}
		return _hash;
	}
	
	@Override 
	public boolean equals(Object o) {
		if(!(o instanceof CNodeCell))
			return false;
		
		CNodeCell that = (CNodeCell)o;
		return super.equals(that) 
			&& _type == that._type
			&& _aggOp == that._aggOp
			&& _sparseSafe == that._sparseSafe
			&& _requiresCastdtm == that._requiresCastdtm
			&& equalInputReferences(
				_output, that._output, _inputs, that._inputs);
	}
	
	@Override
	public String getTemplateInfo() {
		StringBuilder sb = new StringBuilder();
		sb.append("SPOOF CELLWISE [type=");
		sb.append(_type.name());
		sb.append(", aggOp="+((_aggOp!=null) ? _aggOp.name() : "null"));
		sb.append(", sparseSafe="+_sparseSafe);
		sb.append(", castdtm="+_requiresCastdtm);
		sb.append(", mc="+_multipleConsumers);
		sb.append("]");
		return sb.toString();
	}
}
