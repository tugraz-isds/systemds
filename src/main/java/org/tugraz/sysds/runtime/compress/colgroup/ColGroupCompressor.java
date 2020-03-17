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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.compress.BitmapEncoder;
import org.tugraz.sysds.runtime.compress.CompressedMatrixBlock;
import org.tugraz.sysds.runtime.compress.UncompressedBitmap;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroup.CompressionType;
import org.tugraz.sysds.runtime.compress.estim.CompressedSizeEstimator;
import org.tugraz.sysds.runtime.compress.estim.CompressedSizeInfo;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.CommonThreadPool;

public class ColGroupCompressor {

	public static ColGroup[] compressColGroups(MatrixBlock in, CompressedSizeEstimator estimator,
		HashMap<Integer, Double> compRatios, int rlen, List<int[]> groups, boolean denseEst) {
		ColGroup[] ret = new ColGroup[groups.size()];
		for(int i = 0; i < groups.size(); i++)
			ret[i] = compressColGroup(in, estimator, compRatios, rlen, groups.get(i), denseEst);

		return ret;
	}

	protected static ColGroup[] compressColGroups(MatrixBlock in, CompressedSizeEstimator estimator,
		HashMap<Integer, Double> compRatios, int rlen, List<int[]> groups, boolean denseEst, int k) {
		if(k == 1) {
			compressColGroups(in, estimator, compRatios, rlen, groups, denseEst);
		}

		try {
			ExecutorService pool = CommonThreadPool.get(k);
			ArrayList<CompressTask> tasks = new ArrayList<>();
			for(int[] colIndexes : groups)
				tasks.add(new CompressTask(in, estimator, compRatios, rlen, colIndexes, denseEst));
			List<Future<ColGroup>> rtask = pool.invokeAll(tasks);
			ArrayList<ColGroup> ret = new ArrayList<>();
			for(Future<ColGroup> lrtask : rtask)
				ret.add(lrtask.get());
			pool.shutdown();
			return ret.toArray(new ColGroup[0]);
		}
		catch(Exception ex) {
			throw new DMLRuntimeException(ex);
		}
	}

	private static ColGroup compressColGroup(MatrixBlock in, CompressedSizeEstimator estimator,
		HashMap<Integer, Double> compRatios, int rlen, int[] colIndexes, boolean denseEst) {
		int[] allGroupIndices = null;
		int allColsCount = colIndexes.length;
		CompressedSizeInfo sizeInfo;
		// The compression type is decided based on a full bitmap since it
		// will be reused for the actual compression step.
		UncompressedBitmap ubm = null;
		PriorityQueue<CompressedColumn> compRatioPQ = null;
		boolean skipGroup = false;
		while(true) {
			// exact big list and observe compression ratio
			ubm = BitmapEncoder.extractBitmap(colIndexes, in);
			sizeInfo = estimator.estimateCompressedColGroupSize(ubm);
			double sp2 = denseEst ? 1.0 : OptimizerUtils.getSparsity(rlen, 1, ubm.getNumOffsets());
			// System.out.println(MatrixBlock.estimateSizeInMemory(rlen, colIndexes.length, sp2) + " --- "
			// + getUncompressedSize(rlen, colIndexes.length, sp2));
			if(sizeInfo.getMinSize() == 0) {
				throw new DMLRuntimeException("Size info of compressed Col Group is 0");
			}
			// double compRatio = getUncompressedSize(rlen, colIndexes.length, sp2) / sizeInfo.getMinSize();
			double compRatio = MatrixBlock.estimateSizeInMemory(rlen, colIndexes.length, sp2) / sizeInfo.getMinSize();

			if(compRatio > 1) {
				break; // we have a good group
			}

			// modify the group
			if(compRatioPQ == null) {
				// first modification
				allGroupIndices = colIndexes.clone();
				compRatioPQ = new PriorityQueue<>();
				for(int i = 0; i < colIndexes.length; i++)
					compRatioPQ.add(new CompressedColumn(i, compRatios.get(colIndexes[i])));
			}

			// index in allGroupIndices
			int removeIx = compRatioPQ.poll().colIx;
			allGroupIndices[removeIx] = -1;
			allColsCount--;
			if(allColsCount == 0) {
				skipGroup = true;
				break;
			}
			colIndexes = new int[allColsCount];
			// copying the values that do not equal -1
			int ix = 0;
			for(int col : allGroupIndices)
				if(col != -1)
					colIndexes[ix++] = col;
		}

		// add group to uncompressed fallback
		if(skipGroup)
			return null;

		// create compressed column group
		long rleSize = sizeInfo.getCompressionSize(CompressionType.RLE_BITMAP);
		long oleSize = sizeInfo.getCompressionSize(CompressionType.OLE_BITMAP);
		long ddcSize = sizeInfo.getCompressionSize(CompressionType.DDC1);

		if(CompressedMatrixBlock.ALLOW_DDC_ENCODING && ddcSize < rleSize && ddcSize < oleSize) {
			if(ubm.getNumValues() <= 255)
				return new ColGroupDDC1(colIndexes, rlen, ubm);
			else
				return new ColGroupDDC2(colIndexes, rlen, ubm);
		}
		else if(rleSize < oleSize)
			return new ColGroupRLE(colIndexes, rlen, ubm);
		else
			return new ColGroupOLE(colIndexes, rlen, ubm);
	}

	// /**
	// * Compute a conservative estimate of the uncompressed size of a column group.
	// *
	// * @param rlen row length
	// * @param clen column length
	// * @param sparsity the sparsity
	// * @return estimate of uncompressed size of column group
	// */
	// private static double getUncompressedSize(int rlen, int clen, double sparsity) {
		// we estimate the uncompressed size as the minimum of dense representation
		// and representation in csr, which moderately overestimates sparse representations
		// of single columns but helps avoid anomalies with sparse columns that are
		// eventually represented in dense
		// return Math.min(8d * rlen * clen, 4d * rlen + 12d * rlen * clen * sparsity);
	// }

	private static class CompressedColumn implements Comparable<CompressedColumn> {
		final int colIx;
		final double compRatio;

		public CompressedColumn(int colIx, double compRatio) {
			this.colIx = colIx;
			this.compRatio = compRatio;
		}

		@Override
		public int compareTo(CompressedColumn o) {
			return (int) Math.signum(compRatio - o.compRatio);
		}
	}

	private static class CompressTask implements Callable<ColGroup> {
		private final MatrixBlock _in;
		private final CompressedSizeEstimator _estimator;
		private final HashMap<Integer, Double> _compRatios;
		private final int _rlen;
		private final int[] _colIndexes;
		private final boolean _denseEst;

		protected CompressTask(MatrixBlock in, CompressedSizeEstimator estimator, HashMap<Integer, Double> compRatios,
			int rlen, int[] colIndexes, boolean denseEst) {
			_in = in;
			_estimator = estimator;
			_compRatios = compRatios;
			_rlen = rlen;
			_colIndexes = colIndexes;
			_denseEst = denseEst;
		}

		@Override
		public ColGroup call() {
			return compressColGroup(_in, _estimator, _compRatios, _rlen, _colIndexes, _denseEst);
		}
	}

}