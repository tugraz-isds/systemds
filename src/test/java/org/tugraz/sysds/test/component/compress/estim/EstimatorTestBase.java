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

package org.tugraz.sysds.test.component.compress.estim;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.runners.Parameterized.Parameters;
import org.tugraz.sysds.runtime.compress.estim.CompressedSizeEstimator;
import org.tugraz.sysds.runtime.compress.estim.CompressedSizeEstimatorFactory;
import org.tugraz.sysds.test.TestConstants.CompressionType;
import org.tugraz.sysds.test.TestConstants.MatrixType;
import org.tugraz.sysds.test.TestConstants.SparsityType;
import org.tugraz.sysds.test.TestConstants.ValueRange;
import org.tugraz.sysds.test.TestConstants.ValueType;
import org.tugraz.sysds.test.component.compress.TestBase;

public class EstimatorTestBase extends TestBase {

	protected static SparsityType[] usedSparsityTypes = new SparsityType[] {SparsityType.DENSE, SparsityType.SPARSE,
		SparsityType.ULTRA_SPARSE,
		// SparsityType.EMPTY // The empty case should be handled before using the estimator.
	};

	protected static ValueType[] usedValueTypes = new ValueType[] {
		// ValueType.RAND,
		ValueType.CONST, ValueType.RAND_ROUND_DDC, ValueType.RAND_ROUND_OLE,};

	protected static ValueRange[] usedValueRanges = new ValueRange[] {ValueRange.SMALL, ValueRange.LARGE,};

	protected static CompressionType[] usedCompressionTypes = new CompressionType[] {CompressionType.LOSSLESS,
		// CompressionType.LOSSY,
	};

	protected static MatrixType[] usedMatrixType = new MatrixType[] {MatrixType.SMALL,
		// MatrixType.LARGE,
		MatrixType.FEW_COL, MatrixType.FEW_ROW,
		// MatrixType.SINGLE_COL,
		// MatrixType.SINGLE_ROW,
		MatrixType.L_ROWS,
		// MatrixType.XL_ROWS,
	};

	public static double[] samplingRatios = {0.05, 1.00};

	protected static CompressedSizeEstimator cse;

	public EstimatorTestBase(SparsityType sparType, ValueType valType, ValueRange valueRange, CompressionType compType,
		MatrixType matrixType, double samplingRatio) {
		super(sparType, valType, valueRange, compType, matrixType, true, samplingRatio);

		try {

			cse = CompressedSizeEstimatorFactory.getSizeEstimator(mb, true, 3, samplingRatio);
		}
		catch(Exception e) {
			e.printStackTrace();
			assertTrue("Fail Init in Estimator Tests" + this.toString(), false);
		}
	}

	@Parameters
	public static Collection<Object[]> data() {
		ArrayList<Object[]> tests = new ArrayList<>();

		// Only add a single selected test of constructor with no compression

		for(SparsityType st : usedSparsityTypes) {
			for(ValueType vt : usedValueTypes) {
				for(ValueRange vr : usedValueRanges) {
					for(CompressionType ct : usedCompressionTypes) {
						for(MatrixType mt : usedMatrixType) {
							for(double sr : samplingRatios) {
								tests.add(new Object[] {st, vt, vr, ct, mt, sr});
							}
						}
					}
				}
			}
		}

		return tests;
	}

}