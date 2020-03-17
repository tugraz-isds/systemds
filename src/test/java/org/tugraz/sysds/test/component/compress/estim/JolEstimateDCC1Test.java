
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
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupDDC1;
import org.tugraz.sysds.runtime.matrix.data.LibMatrixReorg;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.test.TestUtils;


@RunWith(value = Parameterized.class)
public class JolEstimateDCC1Test {

	@Parameters
	public static Collection<Object[]> data() {
		ArrayList<Object[]> tests = new ArrayList<>();

		tests.add(new Object[] {new double[][] {{1}}, 1, 0});
		tests.add(new Object[] {new double[][] {{1}, {2}}, 2, 0});
		tests.add(new Object[] {new double[][] {{1}, {2}, {3}}, 3, 0});
		tests.add(new Object[] {new double[][] {{1}, {2}, {3}, {4}}, 4, 0});
		tests.add(new Object[] {new double[][] {{1}, {2}, {3}, {4},{5}}, 5, 0});
		tests.add(new Object[] {new double[][] {{1}, {2}, {3}, {4}, {5}, {6}}, 6, 0});
		tests.add(new Object[] {new double[][] {{1}, {2}, {3}, {4}, {5}, {6},{7}, {8}, {9}}, 9, 0});
		tests.add(new Object[] {TestUtils.generateTestMatrix(20, 1, 0, 20, 1.0, 7), 20, 0});
		tests.add(new Object[] {TestUtils.generateTestMatrix(100, 1, 0, 100, 1.0, 7), 100, 0});
		tests.add(new Object[] {TestUtils.generateTestMatrix(101, 1, 0, 100, 1.0, 7), 101, 0});
		// Tolerance is set up because the likelihood of the values to make all the 100 possible values is low.
		tests.add(new Object[] {TestUtils.round(TestUtils.generateTestMatrix(1523, 1, 0, 99, 1.0, 7)), 100, 0});
		tests.add(new Object[] {TestUtils.round(TestUtils.generateTestMatrix(4000, 1, 0, 255, 1.0, 7)), 256, 0});
		tests.add(new Object[] {TestUtils.round(TestUtils.generateTestMatrix(4000, 1, 0, 255, 0.01, 7)),(int) Math.min(4000 * 0.01,256), 50});

		return tests;
	}

	protected final double[][] input;
	protected final long tolerance;
	protected final int numDistinct;

	public JolEstimateDCC1Test(double[][] input, int numDistinct, int tolerance) {
		this.input = input;
		this.numDistinct = numDistinct;
		this.tolerance = tolerance;
	}

	@Test
	public void instanceSize() {
		try {

			MatrixBlock mb = DataConverter.convertToMatrixBlock(input);
			MatrixBlock mbt = !CompressedMatrixBlock.TRANSPOSE_INPUT ? mb : LibMatrixReorg
				.transpose(mb, new MatrixBlock(mb.getNumColumns(), mb.getNumRows(), mb.getSparsity()), 16);

			UncompressedBitmap ubm = BitmapEncoder.extractBitmap(new int[] {0}, mbt );
			ColGroupDDC1 cgU = new ColGroupDDC1(new int[] {0}, input.length, ubm);

			Layouter l = new HotSpotLayouter(new X86_64_DataModel());

			long jolEstimate = 0;
			long diff;

			StringBuilder sb = new StringBuilder();

			for(Object ob : new Object[] {cgU,new int[1], new double[numDistinct], new byte[input.length]}) {
				ClassLayout cl = ClassLayout.parseInstance(ob, l);
				diff =  cl.instanceSize();
				jolEstimate += diff;
				sb.append(cl.toPrintable());
				sb.append("TOTAL MEM: " + jolEstimate  + " diff " + diff + "\n");
			}

			long estimate = cgU.estimateInMemorySize();
			assertTrue(
				"estimate " + estimate + " should be larger than actual " + jolEstimate + " but not more than "
					+ tolerance + " off \n" + sb.toString(),
				estimate >= jolEstimate && jolEstimate >= estimate - tolerance);

		}
		catch(Exception e) {
			e.printStackTrace();
			assertTrue("Failed Test", false);
		}
	}
}