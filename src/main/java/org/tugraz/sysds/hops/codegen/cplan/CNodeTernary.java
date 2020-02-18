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

import java.util.Arrays;

import org.tugraz.sysds.common.Types.DataType;
import org.tugraz.sysds.runtime.util.UtilFunctions;


public class CNodeTernary extends CNode
{
	public enum TernaryType {
		PLUS_MULT, MINUS_MULT,
		BIAS_ADD, BIAS_MULT,
		REPLACE, REPLACE_NAN, IFELSE,
		LOOKUP_RC1, LOOKUP_RVECT1;
		
		public static boolean contains(String value) {
			return Arrays.stream(values()).anyMatch(tt -> tt.name().equals(value));
		}
		
		public String getTemplate(boolean sparse) {
			switch (this) {
				case PLUS_MULT:
					return "    double %TMP% = %IN1% + %IN2% * %IN3%;\n";
				
				case MINUS_MULT:
					return "    double %TMP% = %IN1% - %IN2% * %IN3%;\n";
				
				case BIAS_ADD:
					return "    double %TMP% = %IN1% + getValue(%IN2%, cix/%IN3%);\n";
				
				case BIAS_MULT:
					return "    double %TMP% = %IN1% * getValue(%IN2%, cix/%IN3%);\n";
				
				case REPLACE:
					return "    double %TMP% = (%IN1% == %IN2% || (Double.isNaN(%IN1%) "
							+ "&& Double.isNaN(%IN2%))) ? %IN3% : %IN1%;\n";
				
				case REPLACE_NAN:
					return "    double %TMP% = Double.isNaN(%IN1%) ? %IN3% : %IN1%;\n";
				
				case IFELSE:
					return "    double %TMP% = (%IN1% != 0) ? %IN2% : %IN3%;\n";
				
				case LOOKUP_RC1:
					return sparse ?
						"    double %TMP% = getValue(%IN1v%, %IN1i%, ai, alen, %IN3%-1);\n" :
						"    double %TMP% = getValue(%IN1%, %IN2%, rix, %IN3%-1);\n";
					
				case LOOKUP_RVECT1:
					return "    double[] %TMP% = getVector(%IN1%, %IN2%, rix, %IN3%-1);\n";
					
				default: 
					throw new RuntimeException("Invalid ternary type: "+this.toString());
			}
		}
		
		public boolean isVectorPrimitive() {
			return (this == LOOKUP_RVECT1);
		}
	}
	
	private final TernaryType _type;
	
	public CNodeTernary( CNode in1, CNode in2, CNode in3, TernaryType type ) {
		_inputs.add(in1);
		_inputs.add(in2);
		_inputs.add(in3);
		_type = type;
		setOutputDims();
	}

	public TernaryType getType() {
		return _type;
	}
	
	@Override
	public String codegen(boolean sparse) {
		if( isGenerated() )
			return "";
			
		StringBuilder sb = new StringBuilder();
		
		//generate children
		sb.append(_inputs.get(0).codegen(sparse));
		sb.append(_inputs.get(1).codegen(sparse));
		sb.append(_inputs.get(2).codegen(sparse));
		
		//generate binary operation
		boolean lsparse = sparse && (_inputs.get(0) instanceof CNodeData
			&& _inputs.get(0).getVarname().startsWith("a")
			&& !_inputs.get(0).isLiteral());
		String var = createVarname();
		String tmp = _type.getTemplate(lsparse);
		tmp = tmp.replace("%TMP%", var);
		for( int j=1; j<=3; j++ ) {
			String varj = _inputs.get(j-1).getVarname();
			//replace sparse and dense inputs
			tmp = tmp.replace("%IN"+j+"v%", 
				varj+(varj.startsWith("a")?"vals":"") );
			tmp = tmp.replace("%IN"+j+"i%", 
				varj+(varj.startsWith("a")?"ix":"") );
			tmp = tmp.replace("%IN"+j+"%", varj );
		}
		sb.append(tmp);
		
		//mark as generated
		_generated = true;
		
		return sb.toString();
	}
	
	@Override
	public String toString() {
		switch(_type) {
			case PLUS_MULT:     return "t(+*)";
			case MINUS_MULT:    return "t(-*)";
			case BIAS_ADD:      return "t(bias+)";
			case BIAS_MULT:     return "t(bias*)";
			case REPLACE:
			case REPLACE_NAN:   return "t(rplc)";
			case IFELSE:        return "t(ifelse)";
			case LOOKUP_RC1:    return "u(ixrc1)";
			case LOOKUP_RVECT1: return "u(ixrv1)";
			default:            return super.toString();
		}
	}
	
	@Override
	public void setOutputDims() {
		switch(_type) {
			case PLUS_MULT: 
			case MINUS_MULT:
			case BIAS_ADD:
			case BIAS_MULT:
			case REPLACE:
			case REPLACE_NAN:
			case IFELSE:
			case LOOKUP_RC1:
				_rows = 0;
				_cols = 0;
				_dataType= DataType.SCALAR;
				break;
			case LOOKUP_RVECT1:
				_rows = 1;
				_cols = _inputs.get(0)._cols;
				_dataType= DataType.MATRIX;
				break;
		}
	}
	
	@Override
	public int hashCode() {
		if( _hash == 0 ) {
			_hash = UtilFunctions.intHashCode(
				super.hashCode(), _type.hashCode());
		}
		return _hash;
	}
	
	@Override 
	public boolean equals(Object o) {
		if( !(o instanceof CNodeTernary) )
			return false;
		
		CNodeTernary that = (CNodeTernary) o;
		return super.equals(that)
			&& _type == that._type;
	}
}
