/*
 * Modifications Copyright 2020 Graz University of Technology
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

package org.tugraz.sysds.runtime.instructions.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.tugraz.sysds.runtime.compress.CompressedMatrixBlock;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.tugraz.sysds.runtime.instructions.InstructionUtils;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.data.MatrixIndexes;
import org.tugraz.sysds.runtime.matrix.operators.Operator;

public class CompressionSPInstruction extends UnarySPInstruction {

	private CompressionSPInstruction(Operator op, CPOperand in, CPOperand out, String opcode, String istr) {
		super(SPType.Compression, op, in, out, opcode, istr);
	}

	public static CompressionSPInstruction parseInstruction(String str) {
		InstructionUtils.checkNumFields(str, 2);
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		return new CompressionSPInstruction(null, new CPOperand(parts[1]), new CPOperand(parts[2]), parts[0], str);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		SparkExecutionContext sec = (SparkExecutionContext) ec;

		// get input rdd handle
		JavaPairRDD<MatrixIndexes, MatrixBlock> in = sec.getBinaryMatrixBlockRDDHandleForVariable(input1.getName());

		// execute compression
		JavaPairRDD<MatrixIndexes, MatrixBlock> out = in.mapValues(new CompressionFunction());

		// set outputs
		sec.setRDDHandleForVariable(output.getName(), out);
		sec.addLineageRDD(input1.getName(), output.getName());
	}

	public static class CompressionFunction implements Function<MatrixBlock, MatrixBlock> {
		private static final long serialVersionUID = -6528833083609423922L;

		@Override
		public MatrixBlock call(MatrixBlock arg0) throws Exception {
			return new CompressedMatrixBlock(arg0).compress();
		}
	}
}
