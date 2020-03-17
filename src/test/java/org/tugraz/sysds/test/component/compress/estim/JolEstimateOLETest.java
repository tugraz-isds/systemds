
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
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupOLE;
import org.tugraz.sysds.runtime.matrix.data.LibMatrixReorg;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.test.TestUtils;

@RunWith(value = Parameterized.class)
public class JolEstimateOLETest {

	@Parameters
	public static Collection<Object[]> data() {
		ArrayList<Object[]> tests = new ArrayList<>();

		tests.add(new Object[] {new double[][] {{1}}, 1, 2, 2, 1, 0});

		tests.add(new Object[] {new double[][] {{0}, {0}, {0}, {0}, {5}, {0}}, 1, 2, 2, 1, 0});
		tests.add(new Object[] {TestUtils.generateTestMatrix(100, 1, 0, 100, 1.0, 7), 100, 100 + 1, 200, 100, 0});

		// TODO add real OLE cases.
		
		return tests;
	}

	protected final double[][] input;
	protected final long tolerance;
	protected final int ptRs;
	protected final int numDistinct;
	protected final int dataListSize;
	protected final int skipListSize;

	public JolEstimateOLETest(double[][] input, int numDistinct, int ptRs,  int dataListSize, int skipListSize,
		int tolerance) {
		this.input = input;
		this.ptRs = ptRs;
		this.numDistinct = numDistinct;
		this.tolerance = tolerance;
		this.dataListSize = dataListSize;
		this.skipListSize = skipListSize;
	}

	@Test
	public void instanceSize() {
		try {

			MatrixBlock mb = DataConverter.convertToMatrixBlock(input);
			MatrixBlock mbt = !CompressedMatrixBlock.TRANSPOSE_INPUT ? mb : LibMatrixReorg
				.transpose(mb, new MatrixBlock(mb.getNumColumns(), mb.getNumRows(), mb.getSparsity()), 16);

			UncompressedBitmap ubm = BitmapEncoder.extractBitmap(new int[] {0}, mbt);
			ColGroupOLE cgU = new ColGroupOLE(new int[] {0}, input.length, ubm);

			Layouter l = new HotSpotLayouter(new X86_64_DataModel());

			long jolEstimate = 0;
			long diff;

			StringBuilder sb = new StringBuilder();
			for(Object ob : new Object[] {cgU, new int[1], new double[this.numDistinct], new int[this.ptRs],
				new char[this.dataListSize], new int[skipListSize]}) {
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