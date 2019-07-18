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

package org.tugraz.sysds.runtime.instructions.cp;

import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.lops.TensorGen;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.data.TensorBlock;
import org.tugraz.sysds.runtime.instructions.InstructionUtils;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.operators.Operator;
import org.tugraz.sysds.runtime.util.UtilFunctions;

import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class TensorGenCPInstruction extends UnaryCPInstruction {
	public static final String DELIM = " ";

	private final CPOperand _opDims;
	private final CPOperand _opByRow;

	private TensorGenCPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand in3, CPOperand out,
	                               String opcode, String istr) {
		super(CPType.TensorGen, op, in1, out, opcode, istr);
		_opDims = in2;
		_opByRow = in3;
	}

	public static TensorGenCPInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		InstructionUtils.checkNumFields(parts, 4);
		String opcode = parts[0];
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand in2 = new CPOperand(parts[2]);
		CPOperand in3 = new CPOperand(parts[3]);
		CPOperand out = new CPOperand(parts[4]);
		if(!opcode.equalsIgnoreCase(TensorGen.TENSOR_OPCODE))
			throw new DMLRuntimeException("Unknown opcode while parsing an TensorGenInstruction: " + str);
		else
			return new TensorGenCPInstruction(new Operator(true), in1, in2, in3, out, opcode, str);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		TensorBlock out;
		// TODO use byRow, or remove it
		BooleanObject byRow = (BooleanObject) ec.getScalarInput(_opByRow.getName(), Types.ValueType.BOOLEAN,
							_opByRow.isLiteral());

		//calculate dimensions
		int[] dims;
		// TODO maybe move this branching, depending on what format we got for dims/data, up a level to the HOP and
		//  build multiple LOPs. Requires defined concept for our DAG/HOP/LOP/Instruction structure
		if (_opDims.getDataType() == Types.DataType.MATRIX) {
			// Dimensions given as vector
			MatrixBlock in = ec.getMatrixInput(_opDims.getName(), getExtendedOpcode());
			boolean colVec = false;
			if (in.getNumRows() == 1) {
				colVec = true;
			} else if (!(in.getNumColumns() == 1)) {
				throw new DMLRuntimeException("Dimensions matrix has to be a vector.");
			}
			dims = new int[(int) in.getLength()];
			for (int i = 0; i < in.getLength(); i++) {
				dims[i] = UtilFunctions.toInt(in.getValue(colVec ? 0 : i, colVec ? i : 0));
			}
			ec.releaseMatrixInput(_opDims.getName(), getExtendedOpcode());
		} else if (_opDims.getDataType() == Types.DataType.TENSOR) {
			// Dimensions given as vector
			TensorBlock in = ec.getTensorInput(_opDims.getName());
			boolean colVec = false;
			if (!in.isVector()) {
				throw new DMLRuntimeException("Dimensions tensor has to be a vector.");
			} else if (in.getNumRows() == 1) {
				colVec = true;
			}
			dims = new int[(int) in.getLength()];
			for (int i = 0; i < in.getLength(); i++) {
				dims[i] = UtilFunctions.toInt(in.get(new int[]{colVec ? 0 : i, colVec ? i : 0}));
			}
		} else if (_opDims.getDataType() == Types.DataType.SCALAR && _opDims.getValueType() == Types.ValueType.STRING) {
			// Dimensions given as String
			String dimensionString = ec.getScalarInput(_opDims.getName(), Types.ValueType.STRING, _opDims.isLiteral())
					.getStringValue();
			StringTokenizer dimensions = new StringTokenizer(dimensionString, DELIM);
			dims = new int[dimensions.countTokens()];
			Arrays.setAll(dims, (i) -> Integer.parseInt(dimensions.nextToken()));
		} else if (_opDims.getDataType() == Types.DataType.LIST){
			// Dimensions given as List
			ListObject list = ec.getListObject(_opDims.getName());
			dims = new int[list.getLength()];
			List<Data> dimsData = list.getData();
			for (int i = 0; i < dims.length; i++) {
				if (dimsData.get(i) instanceof ScalarObject) {
					// TODO warning if double value is cast to long?
					dims[i] = (int) ((ScalarObject)dimsData.get(i)).getLongValue();
				} else {
					throw new DMLRuntimeException("Dims parameter for TensorGenCPInstruction does not support " +
							"lists with non scalar values.");
				}
			}
		} else {
			// TODO maybe even support Frame?
			throw new DMLRuntimeException("TensorGenCPInstruction only supports a scalar of type `String` and matrix " +
					"as dimensions parameter.");
		}
		// TODO implement for other ValueTypes
		out = new TensorBlock(Types.ValueType.FP64, dims);
		out.allocateDenseBlock();
		if (input1.getDataType() == Types.DataType.SCALAR) {
			//get Tensor-data as String
			if (input1.getValueType() == Types.ValueType.STRING) {
				String valuesString = ec.getScalarInput(input1).getStringValue();
				StringTokenizer values = new StringTokenizer(valuesString, DELIM);
				int[] ix = new int[dims.length];
				if (values.countTokens() != out.getLength()) {
					throw new DMLRuntimeException("Length of data and length of tensor has to match");
				}
				//fill tensor with data
				for (int i = 0; i < out.getLength(); i++) {
					double d = Double.parseDouble(values.nextToken());
					int j = ix.length - 1;
					out.set(ix, d);
					ix[j]++;
					//calculating next index
					while (ix[j] == dims[j]) {
						ix[j] = 0;
						j--;
						if (j < 0) {
							//we are finished
							break;
						}
						ix[j]++;
					}
				}
			} else {
				// TODO implement for non double ValueTypes
				double value = ec.getScalarInput(input1).getDoubleValue();
				//execute operations
				out.set(value);
			}
		} else if (input1.getDataType() == Types.DataType.TENSOR) {
			//get Tensor-data from tensor (reshape)
			TensorBlock data = ec.getTensorInput(input1.getName());
			out.set(data);
		} else if (input1.getDataType() == Types.DataType.MATRIX) {
			//get Tensor-data from matrix
			MatrixBlock data = ec.getMatrixInput(_opDims.getName(), getExtendedOpcode());
			//execute operations
			out.set(data);
			ec.releaseMatrixInput(_opDims.getName(), getExtendedOpcode());

		} else {
			// TODO support frame and list. Before we implement list it might be good to implement heterogeneous tensors
			throw new DMLRuntimeException("TensorGenCPInstruction only supports scalar (string and number) and tensor" +
					"as data parameter.");
		}

		//set output
		// TODO cacheable tensorOutput
		ec.setTensorOutput(output.getName(), out);
	}
}
