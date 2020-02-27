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
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestUtils;
import org.tugraz.sysds.test.TestConstants;
import org.tugraz.sysds.test.TestConstants.CompressionType;
import org.tugraz.sysds.test.TestConstants.MatrixType;
import org.tugraz.sysds.test.TestConstants.SparsityType;
import org.tugraz.sysds.test.TestConstants.ValueRange;
import org.tugraz.sysds.test.TestConstants.ValueType;

public class CompressedTestBase extends AutomatedTestBase {

	protected static SparsityType[] usedSparsityTypes = new SparsityType[] {
		SparsityType.DENSE, 
		SparsityType.SPARSE,
		// SparsityType.EMPTY
	};
	protected static ValueType[] usedValueTypes = new ValueType[] {
		ValueType.RAND,
		ValueType.CONST,
		ValueType.RAND_ROUND_DDC,
		ValueType.RAND_ROUND_OLE,
	};

	protected static ValueRange[] usedValueRanges = new ValueRange[] {
		ValueRange.SMALL,
		ValueRange.LARGE,
	};

	protected static CompressionType[] usedCompressionTypes = new CompressionType[] {
		CompressionType.LOSSLESS,
		// CompressionType.LOSSY,
	};

	protected static MatrixType[] usedMatrixType = new MatrixType[] {
		MatrixType.SMALL,
		// MatrixType.LARGE,
		MatrixType.FEW_COL,
		MatrixType.FEW_ROW,
		MatrixType.SINGLE_COL,
		MatrixType.SINGLE_ROW,
	};



	protected ValueType valType;
	protected ValueRange valRange;
	protected CompressionType compType;
	protected boolean compress;

	protected int rows;
	protected int cols;
	protected int min;
	protected int max;
	protected double sparsity;

	public CompressedTestBase(SparsityType sparType, ValueType valType, ValueRange valueRange, CompressionType compType,
		MatrixType matrixType, boolean compress) {
		this.sparsity = TestConstants.getSparsityValue(sparType);
		this.rows = TestConstants.getNumberOfRows(matrixType);
		this.cols = TestConstants.getNumberOfColumns(matrixType);

		this.max = TestConstants.getMaxRangeValue(valueRange);
		if(valType == ValueType.CONST) {
			min = max;
		}
		else {
			min = TestConstants.getMinRangeValue(valueRange);
		}
		this.valRange = valueRange;
		this.valType = valType;
		this.compType = compType;
		this.compress = compress;
	}

	protected MatrixBlock getMatrixBlockInput(double[][] input) {
		// generate input data

		if(valType == ValueType.RAND_ROUND_OLE || valType == ValueType.RAND_ROUND_DDC) {
			CompressedMatrixBlock.ALLOW_DDC_ENCODING = (valType == ValueType.RAND_ROUND_DDC);
			input = TestUtils.round(input);
		}

		return DataConverter.convertToMatrixBlock(input);
	}

	@Parameters
	public static Collection<Object[]> data() {
		ArrayList<Object[]> tests = new ArrayList<>();

		// Only add a single selected test of constructor with no compression
		tests.add(new Object[] {SparsityType.SPARSE, ValueType.RAND, ValueRange.SMALL, CompressionType.LOSSLESS,
			MatrixType.SMALL, false});

		for(SparsityType st : usedSparsityTypes) {
			for(ValueType vt : usedValueTypes) {
				for(ValueRange vr : usedValueRanges) {
					for(CompressionType ct : usedCompressionTypes) {
						for(MatrixType mt : usedMatrixType) {
							tests.add(new Object[] {st, vt, vr, ct, mt, true});
						}
					}
				}
			}
		}

		return tests;
	}

	@Override
	public void setUp() {
	}

	@Override
	public void tearDown() {
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		builder.append("args: ");

		builder.append(String.format("%6s%14s","Vt:", valType));
		builder.append(String.format("%6s%8s","Vr:" , valRange));
		builder.append(String.format("%6s%8s","CP:" , compType));
		builder.append(String.format("%6s%5s","CD:" , compress));
		builder.append(String.format("%6s%5s","Rows:" , rows));
		builder.append(String.format("%6s%5s","Cols:" , cols));
		builder.append(String.format("%6s%12s","Min:" , min));
		builder.append(String.format("%6s%12s","Max:" , max));
		builder.append(String.format("%6s%4s","Spar:" , sparsity));

		return builder.toString();
	}
}
