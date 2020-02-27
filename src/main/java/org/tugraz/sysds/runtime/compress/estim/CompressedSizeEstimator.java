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
import org.tugraz.sysds.runtime.compress.CompressedMatrixBlock;
import org.tugraz.sysds.runtime.compress.UncompressedBitmap;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;

public abstract class CompressedSizeEstimator {

	protected MatrixBlock _data;
	protected final int _numRows;

	public CompressedSizeEstimator(MatrixBlock data) {
		_data = data;
		_numRows = CompressedMatrixBlock.TRANSPOSE_INPUT ? _data.getNumColumns() : _data.getNumRows();
	}

	public int getNumRows() {
		return _numRows;
	}

	public abstract CompressedSizeInfo estimateCompressedColGroupSize(int[] colIndexes);

	public abstract CompressedSizeInfo estimateCompressedColGroupSize(UncompressedBitmap ubm);

	protected SizeEstimationFactors computeSizeEstimationFactors(UncompressedBitmap ubm, boolean inclRLE) {
		int numVals = ubm.getNumValues();
		int numRuns = 0;
		int numOffs = 0;
		int numSegs = 0;
		int numSingle = 0;

		// compute size estimation factors
		for(int i = 0; i < numVals; i++) {
			int[] list = ubm.getOffsetsList(i).extractValues();
			int listSize = ubm.getNumOffsets(i);
			numOffs += listSize;
			numSegs += list[listSize - 1] / BitmapEncoder.BITMAP_BLOCK_SZ + 1;
			numSingle += (listSize == 1) ? 1 : 0;
			if(inclRLE) {
				int lastOff = -2;
				for(int j = 0; j < listSize; j++) {
					if(list[j] != lastOff + 1) {
						numRuns++; // new run
						numRuns += (list[j] - lastOff) / // empty runs
							BitmapEncoder.BITMAP_BLOCK_SZ;
					}
					lastOff = list[j];
				}
			}
		}

		// construct estimation factors
		return new SizeEstimationFactors(numVals, numSegs, numOffs, numRuns, numSingle);
	}

	/**
	 * Estimates the number of bytes needed to encode this column group in RLE encoding format.
	 * 
	 * @param numVals number of value tuples
	 * @param numRuns number of runs
	 * @param numCols number of columns
	 * @return number of bytes to encode column group in RLE format
	 */
	protected static long getRLESize(int numVals, int numRuns, int numCols) {
		int ret = 0;
		// distinct value tuples [double per col]
		ret += 8 * numVals * numCols;
		// offset/len fields per distinct value tuple [2xint]
		ret += 8 * numVals;
		// run data [2xchar]
		ret += 4 * numRuns;
		return ret;
	}

	/**
	 * Estimates the number of bytes needed to encode this column group in OLE format.
	 * 
	 * @param numVals number of value tuples
	 * @param numOffs number of offsets
	 * @param numSeqs number of segment headers
	 * @param numCols number of columns
	 * @return number of bytes to encode column group in RLE format
	 */
	protected static long getOLESize(int numVals, float numOffs, int numSeqs, int numCols) {
		int ret = 0;
		// distinct value tuples [double per col]
		ret += 8 * numVals * numCols;
		// offset/len fields per distinct value tuple [2xint]
		ret += 8 * numVals;
		// offset list data [1xchar]
		ret += 2 * numOffs;
		// offset list seqment headers [1xchar]
		ret += 2 * numSeqs;
		return ret;
	}

	/**
	 * Estimates the number of bytes needed to encode this column group in DDC1 or DDC2 format.
	 * 
	 * @param numVals number of value tuples
	 * @param numRows number of rows
	 * @param numCols number of columns
	 * @return number of bytes to encode column group in RLE format
	 */
	protected static long getDDCSize(int numVals, int numRows, int numCols) {
		if(numVals > Character.MAX_VALUE - 1)
			return Long.MAX_VALUE;

		int ret = 0;
		// distinct value tuples [double per col]
		ret += 8 * numVals * numCols;
		// data [byte or char per row]
		ret += ((numVals > 255) ? 2 : 1) * numRows;
		return ret;
	}

	protected static class SizeEstimationFactors {
		protected int numVals; // num value tuples
		protected int numSegs; // num OLE segments
		protected int numOffs; // num OLE offsets
		protected int numRuns; // num RLE runs
		protected int numSingle; // num singletons

		protected SizeEstimationFactors(int numvals, int numsegs, int numoffs, int numruns, int numsingle) {
			numVals = numvals;
			numSegs = numsegs;
			numOffs = numoffs;
			numRuns = numruns;
			numSingle = numsingle;
		}
	}
}
