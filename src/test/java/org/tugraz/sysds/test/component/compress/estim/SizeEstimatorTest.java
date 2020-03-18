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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.tugraz.sysds.test.TestConstants.CompressionType;
import org.tugraz.sysds.test.TestConstants.MatrixType;
import org.tugraz.sysds.test.TestConstants.SparsityType;
import org.tugraz.sysds.test.TestConstants.ValueRange;
import org.tugraz.sysds.test.TestConstants.ValueType;

// import java.lang.instrument.Instrumentation;

@RunWith(value = Parameterized.class)
@net.jcip.annotations.NotThreadSafe
public class SizeEstimatorTest extends EstimatorTestBase {

	public SizeEstimatorTest(SparsityType sparType, ValueType valType, ValueRange valueRange, CompressionType compType,
		MatrixType matrixType, double samplingRatio) {
		super(sparType, valType, valueRange, compType, matrixType, samplingRatio);
	}

	@Test
	public void estimationNot0() {
		long estimate = cse.estimateCompressedColGroupSize().getMinSize();
		assertTrue("Impossible to compress to 0 or negative size " + this.toString(), estimate > 0);
	}

	@Test
	public void estimationBetterThenUncompressed() {
		long estimate = cse.estimateCompressedColGroupSize().getMinSize();
		long uncompressed = mb.getInMemorySize();
		// We allow some epsilon here of A few MB, this is because we do not intend to compress small matrices,
		// and the effect of the compression first shows on larger matrices.
		long eps = 8000;

		assertTrue(
			"Estimated Compression: " + estimate + " is in worst case the uncompressed memory size : " + uncompressed
				+ "\n" + this.toString(),
			estimate < uncompressed + eps);
	}

	// @Test
	// public void OtherInMemoryEstimations() {
	// IObjectProfileNode profile = ObjectProfiler.profile (mb);
	// // long ExternalMeasure = Instrumentation.getObjectSize(mb);
	// long internalMeasure = mb.getInMemorySize();

	// long estimate = cse.estimateCompressedColGroupSize().getMinSize();
	// assertTrue(
	// "Estimated Compression: " + estimate + " is in worst case only 2 times memory than uncompressed: " + uncompressed
	// + "\n"
	// + this.toString(),
	// estimate < uncompressed * 2);
	// }
}