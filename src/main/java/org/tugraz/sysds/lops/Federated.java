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


import java.util.HashMap;

import static org.tugraz.sysds.common.Types.DataType;
import static org.tugraz.sysds.common.Types.ValueType;
import static org.tugraz.sysds.parser.DataExpression.FED_ADDRESSES;
import static org.tugraz.sysds.parser.DataExpression.FED_RANGES;

public class Federated extends Lop {
	private Lop _addresses, _ranges;
	
	public Federated(HashMap<String, Lop> inputLops, DataType dataType, ValueType valueType) {
		super(Type.Federated, dataType, valueType);
		_addresses = inputLops.get(FED_ADDRESSES);
		_ranges = inputLops.get(FED_RANGES);
		
		addInput(_addresses);
		_addresses.addOutput(this);
		addInput(_ranges);
		_ranges.addOutput(this);
	}
	
	@Override
	public String getInstructions(String addresses, String ranges, String output) {
		StringBuilder sb = new StringBuilder("FED");
		sb.append(OPERAND_DELIMITOR);
		sb.append("fedinit");
		sb.append(OPERAND_DELIMITOR);
		sb.append(_addresses.prepScalarInputOperand(addresses));
		sb.append(OPERAND_DELIMITOR);
		sb.append(_ranges.prepScalarInputOperand(ranges));
		sb.append(OPERAND_DELIMITOR);
		sb.append(prepOutputOperand(output));
		return sb.toString();
	}
	
	@Override
	public String toString() {
		// TODO Federated.toString() lop
		return null;
	}
}
