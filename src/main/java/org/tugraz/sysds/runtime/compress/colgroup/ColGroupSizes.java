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

package org.tugraz.sysds.runtime.compress.colgroup;

import org.apache.commons.lang.NotImplementedException;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;

public class ColGroupSizes {

	public static long getEmptyMemoryFootprint(Class<?> colGroupClass) {
		switch(colGroupClass.getSimpleName()) {
			case "ColGroup":
				return estimateInMemorySizeGroup(0);
			case "ColGroupValue":
				return estimateInMemorySizeGroupValue(0, 0);
			case "ColGroupOffset":
				return estimateInMemorySizeOffset(0, 0, 0, 0);
			case "ColGroupDDC":
				return estimateInMemorySizeDDC(0, 0);
			case "ColGroupDDC1":
				return estimateInMemorySizeDDC1(0, 0, 0, 0);
			case "ColGroupDDC2":
				return estimateInMemorySizeDDC2(0, 0, 0, 0);
			case "ColGroupOLE":
				return estimateInMemorySizeOLE(0, 0, 0, 0, 0);
			case "ColGroupRLE":
				return estimateInMemorySizeRLE(0, 0, 0);
			case "ColGroupUncompressed":
				return estimateInMemorySizeUncompressed(0, 0, 0.0);
			default:
				throw new NotImplementedException("Case not implemented");
		}
	}

	/**
	 * @return an upper bound on the number of bytes used to store this ColGroup in memory.
	 */
	public static long estimateInMemorySizeGroup(int nrColumns) {
		long size = 0;
		size += 16; // Object header
		size += 4; // int numRows,
		size += 1; // _zeros boolean reference
		size += 3; // padding

		size += intArrayCost(nrColumns);

		return size;
	}

	public static long estimateInMemorySizeGroupValue(int nrColumns, long nrValues) {
		long size = estimateInMemorySizeGroup(nrColumns);
		size += doubleArrayCost(nrValues);
		return size;
	}

	public static long estimateInMemorySizeDDC(int nrCols, int uniqueVals) {
		if(uniqueVals > Character.MAX_VALUE - 1)
			return Long.MAX_VALUE;
		long size = estimateInMemorySizeGroupValue(nrCols, uniqueVals);
		return size;
	}

	public static long estimateInMemorySizeDDC1(int nrCols, long nrValues, int uniqueVals, int dataLength) {
		long size = estimateInMemorySizeDDC(nrCols, uniqueVals);
		size += byteArrayCost(dataLength);
		return size;
	}

	public static long estimateInMemorySizeDDC2(int nrCols, long nrValues, int uniqueVals, int dataLength) {
		long size = estimateInMemorySizeDDC(nrCols, uniqueVals);
		size += charArrayCost(dataLength);
		return size;
	}

	public static long estimateInMemorySizeOffset(int nrColumns, long nrValues, int pointers, int dataLength) {
		long size = estimateInMemorySizeGroupValue(nrColumns, nrValues);
		size += intArrayCost(pointers);
		size += charArrayCost(dataLength);

		return size;
	}

	public static long estimateInMemorySizeOLE(int nrColumns, long nrValues, int pointers, int dataLength,
		int skipList) {
		long size = estimateInMemorySizeOffset(nrColumns, nrValues, pointers, dataLength);
		size += intArrayCost(skipList);

		return size;
	}

	// TODO Find out if this correlates, the number of pointers and data with number of runs in RLE.
	public static long estimateInMemorySizeRLE(int nrColumns, long nrValues, int numRuns) {
		long size = estimateInMemorySizeOffset(nrColumns, nrValues, numRuns, numRuns);

		return size;
	}

	public static long estimateInMemorySizeUncompressed(int nrRows, int nrColumns, double sparsity) {

		long size = 0;
		// Since the Object is a col group the overhead from the Memory Size group is added
		size += estimateInMemorySizeGroup(nrColumns);

		size += 8; // reference to MatrixBlock.
		size += MatrixBlock.estimateSizeInMemory(nrRows, nrColumns, sparsity);

		return size;
		// return size + 8 + MatrixBlock.estimateSizeInMemory(nrRows, nrColumns, sparsity);
	}

	private static long byteArrayCost(int length) {
		long size = 0;
		size += 8; // Byte array Reference
		size += 20; // Byte array Object header
		if(length <= 4) { // byte array fills out the first 4 bytes differently than the later bytes.
			size += 4;
		}
		else { // byte array pads to next 8 bytes after the first 4.
			size += length;
			int diff = (length - 4) % 8;
			if(diff > 0) {
				size += 8 - diff;
			}
		}
		return size;
	}

	private static long charArrayCost(int length) {
		long size = 0;
		size += 8; // char array Reference
		size += 20; // char array Object header
		if(length <= 2) { // char array fills out the first 2 chars differently than the later bytes.
			size += 4;
		}
		else {
			size += length * 2;// 2 bytes per char
			int diff = (length * 2 - 4) % 8;
			if(diff > 0) {
				size += 8 - diff; // next object alignment
			}
		}
		return size;
	}

	private static long intArrayCost(int length) {
		long size = 0;
		size += 8; // _ptr int[] reference
		size += 20; // int array Object header
		if(length <= 1) {
			size += 4;
		}
		else {
			size += length * 4; // offsets 4 bytes per int
			if(length % 2 == 0) {
				size += 4;
			}
		}
		return size;
	}

	private static long doubleArrayCost(long length) {
		long size = 0;
		size += 8; // _values double array reference
		size += 20; // double array object header
		size += 4; // padding inside double array object to align to 8 bytes.
		size += 8 * length; // Each double fills 8 Bytes
		return size;
	}
}