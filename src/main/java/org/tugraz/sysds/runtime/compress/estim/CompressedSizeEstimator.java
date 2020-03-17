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

public abstract class CompressedSizeEstimator {

	protected MatrixBlock _data;
	protected final int _numRows;
	protected final int _numCols;

	public CompressedSizeEstimator(MatrixBlock data, boolean transpose) {
		_data = data;
		_numRows = transpose ? _data.getNumColumns() : _data.getNumRows();
		_numCols = transpose ? _data.getNumRows() : _data.getNumColumns();
	}

	public CompressedSizeInfo estimateCompressedColGroupSize(){
		int[] colIndexes = new int[_numCols];
		for(int i = 0; i<_numCols; i++){
			colIndexes[i] = i;
		}
		return estimateCompressedColGroupSize(BitmapEncoder.extractBitmap(colIndexes, _data));
	}

	public abstract CompressedSizeInfo estimateCompressedColGroupSize(int[] colIndexes);

	public abstract CompressedSizeInfo estimateCompressedColGroupSize(UncompressedBitmap ubm);

}
