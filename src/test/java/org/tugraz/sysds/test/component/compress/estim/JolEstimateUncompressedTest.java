
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
import org.tugraz.sysds.runtime.compress.CompressedMatrixBlock;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupUncompressed;
import org.tugraz.sysds.runtime.matrix.data.LibMatrixReorg;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.DataConverter;

/**
 * Test for the Uncompressed Col Group, To verify that we estimate the memory usage to be the worst case possible.
 */
@RunWith(value = Parameterized.class)
public class JolEstimateUncompressedTest {

	@Parameters
	public static Collection<Object[]> data() {
		ArrayList<Object[]> tests = new ArrayList<>();

		// Only add a single selected test of constructor with no compression
		tests.add(new Object[] {new double[][] {{1}}});
		// tests.add(new Object[] {new double[][] {{1}, {2}, {3}, {4}, {5}, {6}}});
		// tests.add(new Object[] {TestUtils.generateTestMatrix(1023, 1, 0, 100, 0.7, 7)});
		// tests.add(new Object[] {TestUtils.generateTestMatrix(321, 1, 0, 100, 0.1, 7)});

		return tests;
	}

	protected final double[][] input;
	protected final long tolerance = 0;

	public JolEstimateUncompressedTest(double[][] input) {
		this.input = input;
	}

	@Test
	public void instanceSize() {
		try {

			MatrixBlock mb = DataConverter.convertToMatrixBlock(input);
			MatrixBlock mbt = !CompressedMatrixBlock.TRANSPOSE_INPUT ? mb : LibMatrixReorg
				.transpose(mb, new MatrixBlock(mb.getNumColumns(), mb.getNumRows(), mb.getSparsity()), 16);

			ColGroupUncompressed cgU = new ColGroupUncompressed(new int[] {0}, mbt);

			Layouter l = new HotSpotLayouter(new X86_64_DataModel());

			long jolEstimate = 0;

			StringBuilder sb = new StringBuilder();

			for(Object ob : new Object[] {cgU, // The col group it self
				new int[1], // the Colgroup List of one Column
			}) {
				ClassLayout cl = ClassLayout.parseInstance(ob, l);
				jolEstimate += cl.instanceSize();
				sb.append(cl.toPrintable());

			}

			// Add the Matrix blocks own memory footprint. Assuming that the original matrix block is dense for ease of
			// testing.
			jolEstimate += MatrixBlock.estimateSizeInMemory(input.length, 1, 1.);

			long estimate = cgU.estimateInMemorySize();
			assertTrue(
				"estimate " + estimate + " should be larger than actual " + jolEstimate + " but not more than "
					+ tolerance + "\n" + sb.toString(),
				estimate >= jolEstimate && jolEstimate >= estimate - tolerance);

		}
		catch(Exception e) {
			e.printStackTrace();
			assertTrue("Failed Test", false);
		}
	}
}