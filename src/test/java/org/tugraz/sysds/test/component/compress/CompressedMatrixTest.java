/*
 * Modification Copyright 2020 Graz University of Technology
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

package org.tugraz.sysds.test.component.compress;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.tugraz.sysds.lops.MMTSJ.MMTSJType;
import org.tugraz.sysds.lops.MapMultChain.ChainType;
import org.tugraz.sysds.runtime.compress.CompressedMatrixBlock;
import org.tugraz.sysds.runtime.compress.CompressionStatistics;
import org.tugraz.sysds.runtime.functionobjects.Multiply;
import org.tugraz.sysds.runtime.functionobjects.Plus;
import org.tugraz.sysds.runtime.instructions.InstructionUtils;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.tugraz.sysds.runtime.matrix.operators.AggregateOperator;
import org.tugraz.sysds.runtime.matrix.operators.AggregateUnaryOperator;
import org.tugraz.sysds.runtime.matrix.operators.RightScalarOperator;
import org.tugraz.sysds.runtime.matrix.operators.ScalarOperator;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.test.TestConstants.CompressionType;
import org.tugraz.sysds.test.TestConstants.MatrixType;
import org.tugraz.sysds.test.TestConstants.SparsityType;
import org.tugraz.sysds.test.TestConstants.ValueRange;
import org.tugraz.sysds.test.TestConstants.ValueType;
import org.tugraz.sysds.test.TestUtils;

@RunWith(value = Parameterized.class)
public class CompressedMatrixTest extends CompressedTestBase {

	public CompressedMatrixTest(SparsityType sparType, ValueType valType, ValueRange valRange, CompressionType compType,
		MatrixType matrixType, boolean compress, double samplingRatio) {
		super(sparType, valType, valRange, compType, matrixType, compress, samplingRatio);
	}

	@Test
	public void testConstruction() {
		try {
			if(!(cmbResult instanceof CompressedMatrixBlock)) {
				// TODO Compress EVERYTHING!
				return; // Input was not compressed then just pass test
				// Assert.assertTrue("Compression Failed \n" + this.toString(), false);
			}

			TestUtils.compareMatricesBitAvgDistance(input, deCompressed, rows, cols, 0, 0);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testGetValue() {
		try {
			if(!(cmbResult instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test

			for(int i = 0; i < rows; i++)
				for(int j = 0; j < cols; j++) {
					double ulaVal = input[i][j];
					double claVal = cmb.getValue(i, j); // calls quickGetValue internally
					TestUtils.compareScalarBitsJUnit(ulaVal, claVal, 0); // Should be exactly same value
				}
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testAppend() {
		try {
			if(!(cmbResult instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test

			MatrixBlock vector = DataConverter
				.convertToMatrixBlock(TestUtils.generateTestMatrix(rows, 1, 1, 1, 1.0, 3));

			// matrix-vector uncompressed
			MatrixBlock ret1 = mb.append(vector, new MatrixBlock());

			// matrix-vector compressed
			MatrixBlock ret2 = cmb.append(vector, new MatrixBlock());
			if(compress)
				ret2 = ((CompressedMatrixBlock) ret2).decompress();

			// compare result with input
			double[][] d1 = DataConverter.convertToDoubleMatrix(ret1);
			double[][] d2 = DataConverter.convertToDoubleMatrix(ret2);
			TestUtils.compareMatricesBitAvgDistance(d1, d2, rows, cols + 1, 0, 1);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testMatrixMultChain() {
		try {
			if(!(cmbResult instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test

			MatrixBlock vector1 = DataConverter
				.convertToMatrixBlock(TestUtils.generateTestMatrix(cols, 1, 0, 1, 1.0, 3));

			// ChainType ctype = ChainType.XtwXv;
			// Linear regression .
			for(ChainType ctype : new ChainType[] {ChainType.XtwXv, ChainType.XtXv,
				// ChainType.XtXvy
			}) {

				MatrixBlock vector2 = (ctype == ChainType.XtwXv) ? DataConverter
					.convertToMatrixBlock(TestUtils.generateTestMatrix(rows, 1, 0, 1, 1.0, 3)) : null;

				// matrix-vector uncompressed
				MatrixBlock ret1 = mb.chainMatrixMultOperations(vector1, vector2, new MatrixBlock(), ctype);

				// matrix-vector compressed
				MatrixBlock ret2 = cmb.chainMatrixMultOperations(vector1, vector2, new MatrixBlock(), ctype);

				// compare result with input
				double[][] d1 = DataConverter.convertToDoubleMatrix(ret1);
				double[][] d2 = DataConverter.convertToDoubleMatrix(ret2);
				TestUtils.compareMatricesBitAvgDistance(d1, d2, cols, 1, 512, 15);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testTransposeSelfMatrixMult() {
		try {
			if(!(cmbResult instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test
			// ChainType ctype = ChainType.XtwXv;
			for(MMTSJType mType : new MMTSJType[] {MMTSJType.LEFT,
				// MMTSJType.RIGHT
			}) {
				// matrix-vector uncompressed
				MatrixBlock ret1 = mb.transposeSelfMatrixMultOperations(new MatrixBlock(), mType);

				// matrix-vector compressed
				MatrixBlock ret2 = cmb.transposeSelfMatrixMultOperations(new MatrixBlock(), mType);

				// compare result with input
				double[][] d1 = DataConverter.convertToDoubleMatrix(ret1);
				double[][] d2 = DataConverter.convertToDoubleMatrix(ret2);
				TestUtils.compareMatricesBitAvgDistance(d1, d2, cols, cols, 2048, 20);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testMatrixVectorMult() {
		try {
			if(!(cmbResult instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test

			MatrixBlock vector = DataConverter
				.convertToMatrixBlock(TestUtils.generateTestMatrix(cols, 1, 1, 1, 1.0, 3));

			// Make Operator
			AggregateOperator aop = new AggregateOperator(0, Plus.getPlusFnObject());
			AggregateBinaryOperator abop = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), aop);

			// matrix-vector uncompressed
			MatrixBlock ret1 = mb.aggregateBinaryOperations(mb, vector, new MatrixBlock(), abop);

			// matrix-vector compressed
			MatrixBlock ret2 = cmb.aggregateBinaryOperations(cmb, vector, new MatrixBlock(), abop);

			// compare result with input
			double[][] d1 = DataConverter.convertToDoubleMatrix(ret1);
			double[][] d2 = DataConverter.convertToDoubleMatrix(ret2);
			TestUtils.compareMatricesBitAvgDistance(d1, d2, rows, 1, 1024, 1);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testVectorMatrixMult() {
		try {
			if(!(cmbResult instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test

			MatrixBlock vector = DataConverter
				.convertToMatrixBlock(TestUtils.generateTestMatrix(1, rows, 1, 1, 1.0, 3));

			// Make Operator
			AggregateOperator aop = new AggregateOperator(0, Plus.getPlusFnObject());
			AggregateBinaryOperator abop = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), aop);

			// vector-matrix uncompressed
			MatrixBlock ret1 = mb.aggregateBinaryOperations(vector, mb, new MatrixBlock(), abop);

			// vector-matrix compressed
			MatrixBlock ret2 = cmb.aggregateBinaryOperations(vector, cmb, new MatrixBlock(), abop);

			// compare result with input
			double[][] d1 = DataConverter.convertToDoubleMatrix(ret1);
			double[][] d2 = DataConverter.convertToDoubleMatrix(ret2);
			TestUtils.compareMatricesBitAvgDistance(d1, d2, 1, cols, 10000, 500);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testScalarOperationsSparseUnsafe() {
		try {
			if(!(cmbResult instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test

			// matrix-scalar uncompressed
			ScalarOperator sop = new RightScalarOperator(Plus.getPlusFnObject(), 7);
			MatrixBlock ret1 = mb.scalarOperations(sop, new MatrixBlock());

			// matrix-scalar compressed
			MatrixBlock ret2 = cmb.scalarOperations(sop, new MatrixBlock());
			if(compress)
				ret2 = ((CompressedMatrixBlock) ret2).decompress();

			// compare result with input
			double[][] d1 = DataConverter.convertToDoubleMatrix(ret1);
			double[][] d2 = DataConverter.convertToDoubleMatrix(ret2);

			TestUtils.compareMatricesBitAvgDistance(d1, d2, rows, cols, 150, 1);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testScalarOperations() {
		try {
			if(!(cmbResult instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test

			// matrix-scalar uncompressed
			ScalarOperator sop = new RightScalarOperator(Multiply.getMultiplyFnObject(), 7);
			MatrixBlock ret1 = mb.scalarOperations(sop, new MatrixBlock());

			// matrix-scalar compressed
			MatrixBlock ret2 = cmb.scalarOperations(sop, new MatrixBlock());
			if(compress)
				ret2 = ((CompressedMatrixBlock) ret2).decompress();

			// compare result with input
			double[][] d1 = DataConverter.convertToDoubleMatrix(ret1);
			double[][] d2 = DataConverter.convertToDoubleMatrix(ret2);

			TestUtils.compareMatricesBitAvgDistance(d1, d2, rows, cols, 150, 1);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	// TODO replace with Direction x Types.AggOp
	enum AggType {
		ROWSUMS, COLSUMS, SUM, ROWSUMSSQ, COLSUMSSQ, SUMSQ, ROWMAXS, COLMAXS, MAX, ROWMINS, COLMINS, MIN,
	}

	@Test
	public void testUnaryOperators() {
		try {
			if(!(cmbResult instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test
			for(AggType aggType : AggType.values()) {
				AggregateUnaryOperator auop = null;
				switch(aggType) {
					case SUM:
						auop = InstructionUtils.parseBasicAggregateUnaryOperator("uak+");
						break;
					case ROWSUMS:
						auop = InstructionUtils.parseBasicAggregateUnaryOperator("uark+");
						break;
					case COLSUMS:
						auop = InstructionUtils.parseBasicAggregateUnaryOperator("uack+");
						break;
					case SUMSQ:
						auop = InstructionUtils.parseBasicAggregateUnaryOperator("uasqk+");
						break;
					case ROWSUMSSQ:
						auop = InstructionUtils.parseBasicAggregateUnaryOperator("uarsqk+");
						break;
					case COLSUMSSQ:
						auop = InstructionUtils.parseBasicAggregateUnaryOperator("uacsqk+");
						break;
					case MAX:
						auop = InstructionUtils.parseBasicAggregateUnaryOperator("uamax");
						break;
					case ROWMAXS:
						auop = InstructionUtils.parseBasicAggregateUnaryOperator("uarmax");
						break;
					case COLMAXS:
						auop = InstructionUtils.parseBasicAggregateUnaryOperator("uacmax");
						break;
					case MIN:
						auop = InstructionUtils.parseBasicAggregateUnaryOperator("uamin");
						break;
					case ROWMINS:
						auop = InstructionUtils.parseBasicAggregateUnaryOperator("uarmin");
						break;
					case COLMINS:
						auop = InstructionUtils.parseBasicAggregateUnaryOperator("uacmin");
						break;
				}
				// matrix-vector uncompressed
				MatrixBlock ret1 = mb
					.aggregateUnaryOperations(auop, new MatrixBlock(), Math.max(rows, cols), null, true);

				// matrix-vector compressed
				MatrixBlock ret2 = cmb
					.aggregateUnaryOperations(auop, new MatrixBlock(), Math.max(rows, cols), null, true);

				// compare result with input
				double[][] d1 = DataConverter.convertToDoubleMatrix(ret1);
				double[][] d2 = DataConverter.convertToDoubleMatrix(ret2);
				int dim1 = (aggType == AggType.ROWSUMS || aggType == AggType.ROWSUMSSQ || aggType == AggType.ROWMINS ||
					aggType == AggType.ROWMINS) ? rows : 1;
				int dim2 = (aggType == AggType.COLSUMS || aggType == AggType.COLSUMSSQ || aggType == AggType.COLMAXS ||
					aggType == AggType.COLMINS) ? cols : 1;

				TestUtils.compareMatricesBitAvgDistance(d1, d2, dim1, dim2, 2048, 20);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testSerialization() {
		try {
			if(!(cmbResult instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test

			// serialize compressed matrix block
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			DataOutputStream fos = new DataOutputStream(bos);
			cmb.write(fos);

			// deserialize compressed matrix block
			ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
			DataInputStream fis = new DataInputStream(bis);
			CompressedMatrixBlock cmb2 = new CompressedMatrixBlock();
			cmb2.readFields(fis);

			// decompress the compressed matrix block
			MatrixBlock tmp = cmb2.decompress();

			// compare result with input
			double[][] d1 = DataConverter.convertToDoubleMatrix(mb);
			double[][] d2 = DataConverter.convertToDoubleMatrix(tmp);

			TestUtils.compareMatricesBitAvgDistance(d1, d2, rows, cols, 0, 0);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testCompressionRatio() {
		try {
			if(!(cmbResult instanceof CompressedMatrixBlock))
				return;
			CompressionStatistics cStat = cmb.getCompressionStatistics();

			assertTrue("Compression ration if compressed should be larger than 1", cStat.ratio > 1);

		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testCompressionEstimationVSCompression() {
		try {
			if(!(cmbResult instanceof CompressedMatrixBlock))
				return;

			CompressionStatistics cStat = cmb.getCompressionStatistics();

			long colsEstimate = cStat.estimatedSizeCols;
			long groupsEstimate = cStat.estimatedSizeColGroups;
			long actualSize = cStat.size;
			long originalSize = cStat.originalSize;

			StringBuilder builder = new StringBuilder();
			builder.append("\n\tActual compressed size: " + actualSize);
			builder.append(" should be less than the estimated ColGroup compressed size " + groupsEstimate);
			builder.append(" and both be less than the compressed isolated cols: " + colsEstimate);
			builder.append(" and all less than original size: " + originalSize);
			builder.append("\n\tcol groups types: " + cStat.getGroupsTypesString());
			builder.append("\n\tcol groups sizes: " + cStat.getGroupsSizesString());
			builder.append("\n\t" + this.toString());

			// System.out.println(builder.toString());
			assertTrue(builder.toString(), groupsEstimate >= actualSize && colsEstimate >= groupsEstimate);
			// assertTrue(builder.toString(), groupsEstimate < actualSize && colsEstimate < groupsEstimate);

		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testCompressionScale() {
		// This test is here for a sanity check such that we verify that the compression ratio from our Matrix
		// Compressed Block is not unreasonable.
		try {
			if(!(cmbResult instanceof CompressedMatrixBlock))
				return;

			CompressionStatistics cStat = cmb.getCompressionStatistics();

			double compressRatio = cStat.ratio;
			long actualSize = cStat.size;
			long originalSize = cStat.originalSize;

			StringBuilder builder = new StringBuilder();
			builder.append("Compression Ratio sounds suspiciously good at: " + compressRatio);
			builder.append("\n\tActual compressed size: " + actualSize);
			builder.append(" original size: " + originalSize);
			builder.append("\n\tcol groups types: " + cStat.getGroupsTypesString());
			builder.append("\n\tcol groups sizes: " + cStat.getGroupsSizesString());
			builder.append("\n\t" + this.toString());

			// System.out.println(builder.toString());
			assertTrue(builder.toString(), compressRatio < 100.0);
			// assertTrue(builder.toString(), groupsEstimate < actualSize && colsEstimate < groupsEstimate);

		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

}
