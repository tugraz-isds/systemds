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

package org.tugraz.sysds.runtime.compress.estim;

import org.tugraz.sysds.runtime.compress.BitmapEncoder;
import org.tugraz.sysds.runtime.compress.UncompressedBitmap;

public class CompressedSizeEstimationFactors {
	protected final int numVals; // num value tuples
	protected final int numSegs; // num OLE segments
	protected final int numOffs; // num OLE offsets
	protected final int numRuns; // num RLE runs
	protected final int numSingle; // num singletons
	protected final int numRows;
	protected final int numCols;

	protected CompressedSizeEstimationFactors(int numVals, int numSegs, int numOffs, int numRuns, int numSingle,
		int numRows, int numCols) {
		this.numVals = numVals;
		this.numSegs = numSegs;
		this.numOffs = numOffs;
		this.numRuns = numRuns;
		this.numSingle = numSingle;
		this.numRows = numRows;
		this.numCols = numCols;
	}

	protected static CompressedSizeEstimationFactors computeSizeEstimationFactors(UncompressedBitmap ubm,
		boolean inclRLE, int numRows, int numCols) {
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
		return new CompressedSizeEstimationFactors(numVals, numSegs, numOffs, numRuns, numSingle, numRows, numCols);
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();

		sb.append(" rows:" + numRows);
		sb.append(" cols:" + numCols);
		sb.append(" num Offsets:" + numOffs);
		sb.append(" num Segments:" + numSegs);
		sb.append(" num Singles:" + numSingle);
		sb.append(" num Runs:" + numRuns);
		sb.append(" num Unique Vals:" + numVals);

		return sb.toString();
	}

}