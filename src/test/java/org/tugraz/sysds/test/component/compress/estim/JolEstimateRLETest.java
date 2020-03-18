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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openjdk.jol.datamodel.X86_64_DataModel;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.layouters.HotSpotLayouter;
import org.openjdk.jol.layouters.Layouter;
import org.tugraz.sysds.runtime.compress.BitmapEncoder;
import org.tugraz.sysds.runtime.compress.CompressedMatrixBlock;
import org.tugraz.sysds.runtime.compress.UncompressedBitmap;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupRLE;
import org.tugraz.sysds.runtime.matrix.data.LibMatrixReorg;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.test.TestUtils;


@RunWith(value = Parameterized.class)
public class JolEstimateRLETest {

	@Parameters
	public static Collection<Object[]> data() {
		ArrayList<Object[]> tests = new ArrayList<>();

		tests.add(new Object[] {new double[][] {{1}}, 1, 2, 2, 0});

		// The size of the compression is the same even at different numbers of repeated values.
		tests.add(new Object[] {new double[][] {{0}, {0}, {0}, {0}, {5}, {0}}, 1, 2, 2, 0});
		tests.add(new Object[] {new double[][] {{0}, {0}, {0}, {0}, {5}, {5}, {0}}, 1, 2, 2, 0});
		tests.add(new Object[] {new double[][] {{0}, {0}, {0}, {0}, {5}, {5}, {5}, {0}}, 1, 2, 2, 0});
		tests.add(new Object[] {new double[][] {{0}, {0}, {0}, {0}, {5}, {5}, {5}, {5}, {5}, {5}}, 1, 2, 2, 0});

		// Worst case all random numbers dense.
		tests.add(new Object[] {TestUtils.generateTestMatrix(100, 1, 0, 100, 1.0, 7), 100, 100 + 1, 200, 0});
		tests.add(new Object[] {TestUtils.generateTestMatrix(1000, 1, 0, 100, 1.0, 7), 1000, 1000 + 1, 2000, 0});
		tests.add(new Object[] {TestUtils.generateTestMatrix(10000, 1, 0, 100, 1.0, 7), 10000, 10000 + 1, 20000, 0});

		// Random rounded numbers
		// adding some Tolerance and estimate that it is possible to compress to half size from fully dense in these
		// cases .
		tests.add(new Object[] {TestUtils.round(TestUtils.generateTestMatrix(1523, 1, 0, 99, 1.0, 7)), 100,
			(1523 + 1) / 2, 1523 * 2 / 2, 350});
		tests.add(new Object[] {TestUtils.round(TestUtils.generateTestMatrix(4000, 1, 0, 255, 1.0, 7)), 256,
			(4000 + 1) / 2, 4000 * 2 / 2, 1000});

		// Sparse rounded numbers
		// Scale directly with sparsity
		tests.add(new Object[] {TestUtils.round(TestUtils.generateTestMatrix(1523, 1, 0, 99, 0.1, 7)), 100,
			(1523 + 1) / 2 /10, 1523 * 2 / 2 / 10, 100});
		tests.add(new Object[] {TestUtils.round(TestUtils.generateTestMatrix(4000, 1, 0, 255, 0.1, 7)), 256,
		 	(4000 + 1) / 2 / 10, 4000 * 2 / 2 / 10, 250});

		return tests;
	}

	protected final double[][] input;
	protected final long tolerance;
	protected final int numDistinct;
	protected final int ptRs;
	protected final int dataListSize;

	public JolEstimateRLETest(double[][] input, int numDistinct, int ptRs, int dataListSize, int tolerance) {
		this.input = input;
		this.ptRs = ptRs;
		this.numDistinct = numDistinct;
		this.tolerance = tolerance;
		this.dataListSize = dataListSize;
	}

	@Test
	public void instanceSize() {
		try {

			MatrixBlock mb = DataConverter.convertToMatrixBlock(input);
			MatrixBlock mbt = !CompressedMatrixBlock.TRANSPOSE_INPUT ? mb : LibMatrixReorg
				.transpose(mb, new MatrixBlock(mb.getNumColumns(), mb.getNumRows(), mb.getSparsity()), 16);

			UncompressedBitmap ubm = BitmapEncoder.extractBitmap(new int[] {0}, mbt);
			ColGroupRLE cgU = new ColGroupRLE(new int[] {0}, input.length, ubm);

			Layouter l = new HotSpotLayouter(new X86_64_DataModel());

			long jolEstimate = 0;
			long diff;

			StringBuilder sb = new StringBuilder();
			for(Object ob : new Object[] {cgU, new int[1], new double[this.numDistinct], new int[this.ptRs],
				new char[this.dataListSize]}) {
				ClassLayout cl = ClassLayout.parseInstance(ob, l);
				diff = cl.instanceSize();
				jolEstimate += diff;
				sb.append(cl.toPrintable());
				sb.append("TOTAL MEM: " + jolEstimate + " diff " + diff + "\n");

			}

			long estimate = cgU.estimateInMemorySize();

			String errorMessage = "estimate " + estimate + " should be larger than actual " + jolEstimate
				+ " but not more than " + tolerance + " off \n";
			assertTrue(errorMessage + sb.toString() + "\n" + errorMessage,
				estimate >= jolEstimate && jolEstimate >= estimate - tolerance);

		}
		catch(Exception e) {
			e.printStackTrace();
			assertTrue("Failed Test", false);
		}
	}
}