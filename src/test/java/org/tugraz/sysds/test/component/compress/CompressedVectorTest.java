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

package org.tugraz.sysds.test.component.compress;

import org.tugraz.sysds.runtime.compress.CompressedMatrixBlock;
import org.tugraz.sysds.runtime.functionobjects.CM;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.operators.CMOperator;
import org.tugraz.sysds.runtime.matrix.operators.CMOperator.AggregateOperationTypes;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.test.TestUtils;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.tugraz.sysds.test.TestConstants.CompressionType;
import org.tugraz.sysds.test.TestConstants.MatrixType;
import org.tugraz.sysds.test.TestConstants.SparsityType;
import org.tugraz.sysds.test.TestConstants.ValueType;
import org.tugraz.sysds.test.TestConstants.ValueRange;

@RunWith(value = Parameterized.class)
public class CompressedVectorTest extends CompressedTestBase {

	// Input
	double[][] input;
	MatrixBlock mb;

	// Compressed Block
	CompressedMatrixBlock cmb;

	// Compression Result
	MatrixBlock cmbResult;

	// Decompressed Result
	MatrixBlock cmbDeCompressed;
	double[][] deCompressed;

	protected static MatrixType[] usedMatrixType = new MatrixType[] {
		MatrixType.SINGLE_COL
	};

	@Parameters
	public static Collection<Object[]> data() {
		ArrayList<Object[]> tests = new ArrayList<>();
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

	public CompressedVectorTest(SparsityType sparType, ValueType valType, ValueRange valRange, CompressionType compType,
		MatrixType matrixType, boolean compress) {
		super(sparType, valType, valRange, compType, matrixType, compress);
		input = TestUtils.generateTestMatrix(rows, cols, min, max, sparsity, 7);
		mb = getMatrixBlockInput(input);
		cmb = new CompressedMatrixBlock(mb);
		if(compress) {
			cmbResult = cmb.compress();
		}
		cmbDeCompressed = cmb.decompress();
		deCompressed = DataConverter.convertToDoubleMatrix(cmbDeCompressed);
	}

	@Test
	public void testCentralMoment() throws Exception {
		// TODO: Make Central Moment Test work on Multi dimensional Matrix
		try {
			if(!(cmbResult instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test
			

			// quantile uncompressed
			AggregateOperationTypes opType = CMOperator.getCMAggOpType(2);
			CMOperator cm = new CMOperator(CM.getCMFnObject(opType), opType);

			double ret1 = mb.cmOperations(cm).getRequiredResult(opType);

			// quantile compressed
			double ret2 = cmb.cmOperations(cm).getRequiredResult(opType);
			// compare result with input allowing 1 bit difference in least significant location
			TestUtils.compareScalarBitsJUnit(ret1, ret2, 1);

		}
		catch(Exception e) {
			throw new Exception(this.toString() + "\n" + e.getMessage(), e);
		}
		finally {
			CompressedMatrixBlock.ALLOW_DDC_ENCODING = true;
		}
	}

	@Test
	public void testQuantile() {
		try {
			// quantile uncompressed
			MatrixBlock tmp1 = (MatrixBlock) mb.sortOperations(null, new MatrixBlock());
			double ret1 = tmp1.pickValue(0.95);

			// quantile compressed
			MatrixBlock tmp2 = (MatrixBlock) cmb.sortOperations(null, new MatrixBlock());
			double ret2 = tmp2.pickValue(0.95);

			// compare result with input
			TestUtils.compareScalars(ret1, ret2, 0.0000001);
		}
		catch(Exception e) {
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}
}
