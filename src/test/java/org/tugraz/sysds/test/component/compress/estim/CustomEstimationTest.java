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

import java.util.Map;

import org.junit.Test;
import org.tugraz.sysds.runtime.compress.BitmapEncoder;
import org.tugraz.sysds.runtime.compress.CompressedMatrixBlock;
import org.tugraz.sysds.runtime.compress.UncompressedBitmap;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroup.CompressionType;
import org.tugraz.sysds.runtime.compress.estim.CompressedSizeEstimator;
import org.tugraz.sysds.runtime.compress.estim.CompressedSizeEstimatorFactory;
import org.tugraz.sysds.runtime.compress.estim.CompressedSizeInfo;
import org.tugraz.sysds.runtime.matrix.data.LibMatrixReorg;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.DataConverter;

public class CustomEstimationTest {

	// Compressed Linear Algebra for Large-Scale Machine Learning VLDB 2016

	double[][] paper = {{7, 9, 6, 2.1, 0.99}, {3, 9, 4, 3, 0.73}, {7, 9, 6, 2.1, 0.05}, {7, 9, 5, 3, 0.42},
		{3, 0, 4, 2.1, 0.61}, {7, 8.2, 5, 3, 0.89}, {3, 9, 4, 3, 0.07}, {3, 9, 4, 0, 0.92}, {7, 9, 6, 2.1, 0.54},
		{3, 0, 4, 3, 0.16}};

	double[][] paper2 = {{7, 9, 6, 2.1, 0.99}, {3, 9, 4, 3, 0.73}, {7, 9, 6, 2.1, 0.05}, {7, 9, 5, 3, 0.42},
		{3, 0, 4, 2.1, 0.61}, {7, 8.2, 5, 3, 0.89}, {3, 9, 4, 3, 0.07}, {3, 9, 4, 0, 0.92}, {7, 9, 6, 2.1, 0.54},
		{3, 0, 4, 3, 0.16}, {7, 9, 6, 2.1, 0.99}, {3, 9, 4, 3, 0.73}, {7, 9, 6, 2.1, 0.05}, {7, 9, 5, 3, 0.42},
		{3, 0, 4, 2.1, 0.61}, {7, 8.2, 5, 3, 0.89}, {3, 9, 4, 3, 0.07}, {3, 9, 4, 0, 0.92}, {7, 9, 6, 2.1, 0.54},
		{3, 0, 4, 3, 0.16}, {7, 9, 6, 2.1, 0.99}, {3, 9, 4, 3, 0.73}, {7, 9, 6, 2.1, 0.05}, {7, 9, 5, 3, 0.42},
		{3, 0, 4, 2.1, 0.61}, {7, 8.2, 5, 3, 0.89}, {3, 9, 4, 3, 0.07}, {3, 9, 4, 0, 0.92}, {7, 9, 6, 2.1, 0.54},
		{3, 0, 4, 3, 0.16}, {7, 9, 6, 2.1, 0.99}, {3, 9, 4, 3, 0.73}, {7, 9, 6, 2.1, 0.05}, {7, 9, 5, 3, 0.42},
		{3, 0, 4, 2.1, 0.61}, {7, 8.2, 5, 3, 0.89}, {3, 9, 4, 3, 0.07}, {3, 9, 4, 0, 0.92}, {7, 9, 6, 2.1, 0.54},
		{3, 0, 4, 3, 0.16}, {7, 9, 6, 2.1, 0.99}, {3, 9, 4, 3, 0.73}, {7, 9, 6, 2.1, 0.05}, {7, 9, 5, 3, 0.42},
		{3, 0, 4, 2.1, 0.61}, {7, 8.2, 5, 3, 0.89}, {3, 9, 4, 3, 0.07}, {3, 9, 4, 0, 0.92}, {7, 9, 6, 2.1, 0.54},
		{3, 0, 4, 3, 0.16}, {7, 9, 6, 2.1, 0.99}, {3, 9, 4, 3, 0.73}, {7, 9, 6, 2.1, 0.05}, {7, 9, 5, 3, 0.42},
		{3, 0, 4, 2.1, 0.61}, {7, 8.2, 5, 3, 0.89}, {3, 9, 4, 3, 0.07}, {3, 9, 4, 0, 0.92}, {7, 9, 6, 2.1, 0.54},
		{3, 0, 4, 3, 0.16}, {7, 9, 6, 2.1, 0.99}, {3, 9, 4, 3, 0.73}, {7, 9, 6, 2.1, 0.05}, {7, 9, 5, 3, 0.42},
		{3, 0, 4, 2.1, 0.61}, {7, 8.2, 5, 3, 0.89}, {3, 9, 4, 3, 0.07}, {3, 9, 4, 0, 0.92}, {7, 9, 6, 2.1, 0.54},
		{3, 0, 4, 3, 0.16}, {7, 9, 6, 2.1, 0.99}, {3, 9, 4, 3, 0.73}, {7, 9, 6, 2.1, 0.05}, {7, 9, 5, 3, 0.42},
		{3, 0, 4, 2.1, 0.61}, {7, 8.2, 5, 3, 0.89}, {3, 9, 4, 3, 0.07}, {3, 9, 4, 0, 0.92}, {7, 9, 6, 2.1, 0.54},
		{3, 0, 4, 3, 0.16}, {7, 9, 6, 2.1, 0.99}, {3, 9, 4, 3, 0.73}, {7, 9, 6, 2.1, 0.05}, {7, 9, 5, 3, 0.42},
		{3, 0, 4, 2.1, 0.61}, {7, 8.2, 5, 3, 0.89}, {3, 9, 4, 3, 0.07}, {3, 9, 4, 0, 0.92}, {7, 9, 6, 2.1, 0.54},
		{3, 0, 4, 3, 0.16}, {7, 9, 6, 2.1, 0.99}, {3, 9, 4, 3, 0.73}, {7, 9, 6, 2.1, 0.05}, {7, 9, 5, 3, 0.42},
		{3, 0, 4, 2.1, 0.61}, {7, 8.2, 5, 3, 0.89}, {3, 9, 4, 3, 0.07}, {3, 9, 4, 0, 0.92}, {7, 9, 6, 2.1, 0.54},
		{3, 0, 4, 3, 0.16}};

	@Test
	public void paperTest() {
		runTest(paper2);
	}

	// @Test
	// public void oleTest() {
	// // From paper.
	// double[][] input = {{2.1}, {3}, {2.1}, {3}, {2.1}, {3}, {3}, {0}, {2.1}, {3}, {2.1}, {3}, {2.1}, {3}, {2.1},
	// {3}, {2.1}, {3}, {3}, {0}, {2.1}, {3}, {2.1}, {3}, {2.1}, {3}, {2.1}, {3}, {2.1}, {3}, {3}, {0}, {2.1}, {3},
	// {2.1}, {3}, {2.1}, {3}, {2.1}, {3}, {2.1}, {3}, {3}, {0}, {2.1}, {3}, {2.1}, {3}, {2.1}, {3}, {2.1}, {3},
	// {2.1}, {3}, {3}, {0}, {2.1}, {3}, {2.1}, {3}, {2.1}, {3}, {2.1}, {3}, {2.1}, {3}, {3}, {0}, {2.1}, {3},
	// {2.1}, {3}, {2.1}, {3}, {2.1}, {3}, {2.1}, {3}, {3}, {0}, {2.1}, {3}, {2.1}, {3}, {2.1}, {3}, {2.1}, {3},
	// {2.1}, {3}, {3}, {0}, {2.1}, {3}, {2.1}, {3}, {2.1}, {3}, {2.1}, {3}, {2.1}, {3}, {3}, {0}, {2.1}, {3},
	// {2.1}, {3}};

	// runTest(input);
	// }

	// @Test
	// public void RLETest() {
	// // From paper.
	// double[][] input = {{9, 9, 9, 9, 0, 8.2, 9, 9, 9, 0}};

	// runTest(input);
	// }

	private void runTest(double[][] input) {
		MatrixBlock mb = DataConverter.convertToMatrixBlock(input);
		MatrixBlock mbt = !CompressedMatrixBlock.TRANSPOSE_INPUT ? mb : LibMatrixReorg
			.transpose(mb, new MatrixBlock(mb.getNumColumns(), mb.getNumRows(), mb.getSparsity()), 16);
		// System.out.println(mb.toString());
		CompressedSizeEstimator cse = CompressedSizeEstimatorFactory.getSizeEstimator(mbt, true, 1, 1.0);
		for(int col = 0; col < 5; col++) {
			CompressedSizeInfo csi = cse.estimateCompressedColGroupSize(new int[] {col});
			Map<CompressionType, Long> comp = csi.getAllCompressionSizes();
			for(CompressionType ct : comp.keySet()) {
				System.out.print("\t" + ct + " -- " + comp.get(ct));
			}
			System.out.println();
		}
		System.out.println();
		CompressedMatrixBlock cmb = new CompressedMatrixBlock(mb);
		cmb.setSeed(1);
		cmb.setSamplingRatio(1.0);
		assertTrue("The input did not compress.", cmb.compress(1) instanceof CompressedMatrixBlock);
		long compMem = cmb.estimateCompressedSizeInMemory();
		System.out.println(cmb.getCompressionStatistics().toString());
		System.out.println(compMem);

	}

	/**
	 * Test To verify that the number of cols is correct in the compression estimation
	 */
	@Test(expected = IndexOutOfBoundsException.class)
	public void testIndexOutOfBoundsException() {
		MatrixBlock mb = DataConverter.convertToMatrixBlock(paper);
		mb = !CompressedMatrixBlock.TRANSPOSE_INPUT ? mb : LibMatrixReorg
			.transpose(mb, new MatrixBlock(mb.getNumColumns(), mb.getNumRows(), mb.getSparsity()), 16);
		// System.out.println(mb.toString());
		CompressedSizeEstimator cse = CompressedSizeEstimatorFactory.getSizeEstimator(mb, true, 1, 1.0);
		int col = 6; // non existing column
		cse.estimateCompressedColGroupSize(new int[] {col});
	}

	@Test
	public void getUncompressedBitMap(){
		MatrixBlock mb = DataConverter.convertToMatrixBlock(paper);
		mb = !CompressedMatrixBlock.TRANSPOSE_INPUT ? mb : LibMatrixReorg
			.transpose(mb, new MatrixBlock(mb.getNumColumns(), mb.getNumRows(), mb.getSparsity()), 16);
		UncompressedBitmap ubm = BitmapEncoder.extractBitmap(new int[] {0}, mb);
		assertTrue("The number of columns should be the same as the extracted", ubm.getNumColumns() == 1);
		assertTrue("The number of destinct values should be " + 2 + " but is " + ubm.getNumValues(), ubm.getNumValues() == 2);
		System.out.println(ubm.toString());
	}

}