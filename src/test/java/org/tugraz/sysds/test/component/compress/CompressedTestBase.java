/*
 * Copyright 2020 Graz University of Technology
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

import java.util.ArrayList;
import java.util.Collection;

import org.junit.runners.Parameterized.Parameters;
import org.tugraz.sysds.runtime.compress.CompressedMatrixBlock;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.test.TestConstants.CompressionType;
import org.tugraz.sysds.test.TestConstants.MatrixType;
import org.tugraz.sysds.test.TestConstants.SparsityType;
import org.tugraz.sysds.test.TestConstants.ValueRange;
import org.tugraz.sysds.test.TestConstants.ValueType;

public class CompressedTestBase extends TestBase {

	protected static SparsityType[] usedSparsityTypes = new SparsityType[] { // Sparsity 0.9, 0.1, 0.01 and 0.0
		SparsityType.DENSE,
		SparsityType.SPARSE, 
		SparsityType.ULTRA_SPARSE,
		// SparsityType.EMPTY
	};
	protected static ValueType[] usedValueTypes = new ValueType[] {
		ValueType.RAND,
		// ValueType.CONST,
		ValueType.RAND_ROUND_DDC, 
		ValueType.RAND_ROUND_OLE};

	protected static ValueRange[] usedValueRanges = new ValueRange[] {ValueRange.SMALL,
		// ValueRange.LARGE,
	};

	protected static CompressionType[] usedCompressionTypes = new CompressionType[] {CompressionType.LOSSLESS,
		// CompressionType.LOSSY,
	};

	protected static MatrixType[] usedMatrixType = new MatrixType[] { // Selected Matrix Types
		MatrixType.SMALL,
		// MatrixType.LARGE,
		MatrixType.FEW_COL,
		MatrixType.FEW_ROW,
		// MatrixType.SINGLE_COL,
		// MatrixType.SINGLE_ROW,
		MatrixType.L_ROWS,
		// MatrixType.XL_ROWS,
	};

	public static double[] samplingRatios = {0.05, 1.00};

	// Compressed Block
	protected CompressedMatrixBlock cmb;

	// Compression Result
	protected MatrixBlock cmbResult;

	// Decompressed Result
	protected MatrixBlock cmbDeCompressed;
	protected double[][] deCompressed;

	// Threads
	protected int k = 1;

	public CompressedTestBase(SparsityType sparType, ValueType valType, ValueRange valueRange, CompressionType compType,
		MatrixType matrixType, boolean compress, double samplingRatio) {
		super(sparType, valType, valueRange, compType, matrixType, compress, samplingRatio);
		try {
			cmb = new CompressedMatrixBlock(mb);
			cmb.setSeed(1);
			cmb.setSamplingRatio(samplingRatio);
			if(compType == CompressionType.LOSSY) {
				cmb.setLossy(true);
			}
			if(compress) {
				cmbResult = cmb.compress(k);
			}
			cmbDeCompressed = cmb.decompress();
			deCompressed = DataConverter.convertToDoubleMatrix(cmbDeCompressed);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(
				"CompressionTest Init failed with settings: " + this.toString() + "\n" + e.getMessage(), e);
			// assertTrue("CompressionTest Init failed with settings: " + this.toString() ,false);
		}

	}

	@Parameters
	public static Collection<Object[]> data() {
		ArrayList<Object[]> tests = new ArrayList<>();

		// Only add a single selected test of constructor with no compression
		tests.add(new Object[] {SparsityType.SPARSE, ValueType.RAND, ValueRange.SMALL, CompressionType.LOSSLESS,
			MatrixType.SMALL, false, 1.0});

		for(SparsityType st : usedSparsityTypes) {
			for(ValueType vt : usedValueTypes) {
				for(ValueRange vr : usedValueRanges) {
					for(CompressionType ct : usedCompressionTypes) {
						for(MatrixType mt : usedMatrixType) {
							for(double sr : samplingRatios) {
								tests.add(new Object[] {st, vt, vr, ct, mt, true, sr});
							}
						}
					}
				}
			}
		}

		return tests;
	}
}
