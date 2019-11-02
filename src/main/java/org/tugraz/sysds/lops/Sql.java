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
 *
 */

package org.tugraz.sysds.lops;

import org.tugraz.sysds.parser.DataExpression;

import java.util.HashMap;

import static org.tugraz.sysds.common.Types.*;
import static org.tugraz.sysds.parser.DataExpression.*;

public class Sql extends Lop {
	private HashMap<String, Lop> _inputParams;
	
	public Sql(HashMap<String, Lop> inputParametersLops, DataType dt, ValueType vt) {
		super(Lop.Type.Sql, dt, vt);
		
		_inputParams = inputParametersLops;
		
		Lop lop = inputParametersLops.get(SQL_CONN);
		addInput(lop);
		lop.addOutput(this);
		lop = inputParametersLops.get(SQL_USER);
		addInput(lop);
		lop.addOutput(this);
		lop = inputParametersLops.get(SQL_PASS);
		addInput(lop);
		lop.addOutput(this);
		lop = inputParametersLops.get(SQL_QUERY);
		addInput(lop);
		lop.addOutput(this);
	}
	
	@Override
	public String getInstructions(String input1, String input2, String input3, String input4, String output) {
		StringBuilder sb = new StringBuilder();
		// TODO spark
		sb.append("CP");
		sb.append(OPERAND_DELIMITOR);
		sb.append("sql");
		sb.append(OPERAND_DELIMITOR);
		Lop inLop = _inputParams.get(SQL_CONN);
		boolean literal = (inLop instanceof Data && ((Data) inLop).isLiteral());
		sb.append(prepOperand(input1, DataType.SCALAR, ValueType.STRING, literal));
		sb.append(OPERAND_DELIMITOR);
		inLop = _inputParams.get(DataExpression.SQL_USER);
		literal = (inLop instanceof Data && ((Data) inLop).isLiteral());
		sb.append(prepOperand(input2, DataType.SCALAR, ValueType.STRING, literal));
		sb.append(OPERAND_DELIMITOR);
		inLop = _inputParams.get(DataExpression.SQL_PASS);
		literal = (inLop instanceof Data && ((Data) inLop).isLiteral());
		sb.append(prepOperand(input3, DataType.SCALAR, ValueType.STRING, literal));
		sb.append(OPERAND_DELIMITOR);
		inLop = _inputParams.get(DataExpression.SQL_QUERY);
		literal = (inLop instanceof Data && ((Data) inLop).isLiteral());
		sb.append(prepOperand(input4, DataType.SCALAR, ValueType.STRING, literal));
		sb.append(OPERAND_DELIMITOR);
		sb.append(prepOutputOperand(output));
		return sb.toString();
	}
	
	@Override
	public String toString() {
		// TODO Sql.toString() lop
		return null;
	}
}
