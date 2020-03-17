/*
 * Modifications Copyright 2020 Graz University of Technology
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

package org.tugraz.sysds.runtime.compress.estim;

import org.tugraz.sysds.runtime.compress.BitmapEncoder;
import org.tugraz.sysds.runtime.compress.UncompressedBitmap;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;

/**
 * Exact compressed size estimator (examines entire dataset).
 * 
 */
public class CompressedSizeEstimatorExact extends CompressedSizeEstimator {
	public CompressedSizeEstimatorExact(MatrixBlock data, boolean transpose) {
		super(data, transpose);
	}

	@Override
	public CompressedSizeInfo estimateCompressedColGroupSize(int[] colIndexes) {
		return estimateCompressedColGroupSize(BitmapEncoder.extractBitmap(colIndexes, _data));
	}

	@Override
	public CompressedSizeInfo estimateCompressedColGroupSize(UncompressedBitmap ubm) {
		// compute size estimation factors
		CompressedSizeEstimationFactors fact = CompressedSizeEstimationFactors.computeSizeEstimationFactors(ubm, true, _numRows, _numCols);

		// construct new size info summary
		return new CompressedSizeInfo(fact);
		
	}
}
