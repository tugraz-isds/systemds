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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroup.CompressionType;
import org.tugraz.sysds.runtime.compress.BitmapEncoder;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupSizes;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.CommonThreadPool;

/**
 * A helper reusable object for maintaining bitmap sizes
 */
public class CompressedSizeInfo {

	private static final Log LOG = LogFactory.getLog(CompressedSizeInfo.class.getName());

	private final int _estCard;
	private final int _estNnz;
	// TODO Maybe only store the best compression?
	private Map<CompressionType, Long> _sizes;

	public CompressedSizeInfo(CompressedSizeEstimationFactors fact) {
		_estCard = fact.numVals;
		_estNnz = fact.numOffs;
		_sizes = calculateCompressionSizes(fact);
	}

	public long getCompressionSize(CompressionType ct) {
		return _sizes.get(ct);
	}

	public Map<CompressionType, Long> getAllCompressionSizes() {
		return _sizes;
	}

	public long getMinSize() {
		return Collections.min(_sizes.values());
	}

	public int getEstCard() {
		return _estCard;
	}

	public int getEstNnz() {
		return _estNnz;
	}

	public static CompressedSizeInfo[] computeCompressedSizeInfos(CompressedSizeEstimator bitmapSizeEstimator,
		List<Integer> colsC, List<Integer> colsUC, HashMap<Integer, Double> compRatios, long nnzUC, int numRows,
		int numCols, int k) {

		CompressedSizeInfo[] sizeInfos = (k > 1) ? computeCompressedSizeInfos(bitmapSizeEstimator,
			numCols,
			k) : computeCompressedSizeInfos(bitmapSizeEstimator, numCols);

		for(int col = 0; col < numCols; col++) {
			double sparsity = OptimizerUtils.getSparsity(numRows, 1, sizeInfos[col].getEstNnz());
			// double uncompSize = MatrixBlock.estimateSizeInMemory(numRows, 1, sparsity);
			double uncompSize = (double) ColGroupSizes.estimateInMemorySizeUncompressed(numRows, 1, sparsity);
			// OptimizerUtils.getSparsity(numRows, 1, sizeInfos[col].getEstNnz()));
			double minCompressedSize = (double) sizeInfos[col].getMinSize();
			double compRatio = uncompSize / minCompressedSize;
			LOG.warn("CompressionRatio:" + compRatio + " // " + uncompSize + "/" + minCompressedSize);
			if(compRatio > 1) {
				colsC.add(col);
				compRatios.put(col, compRatio);
			}
			else {
				colsUC.add(col);
				nnzUC += sizeInfos[col].getEstNnz();
			}
		}

		// correction of column classification (reevaluate dense estimates if necessary)
		if(!MatrixBlock.evalSparseFormatInMemory(numRows, colsUC.size(), nnzUC) && !colsUC.isEmpty()) {
			for(int i = 0; i < colsUC.size(); i++) {
				int col = colsUC.get(i);
				double uncompSize = MatrixBlock.estimateSizeInMemory(numRows, 1, 1.0);
				// CompressedMatrixBlock.getUncompressedSize(numRows, 1, 1.0);
				double compRatio = uncompSize / sizeInfos[col].getMinSize();
				if(compRatio > 1) {
					colsC.add(col);
					colsUC.remove(i);
					i--;
					compRatios.put(col, compRatio);
					nnzUC -= sizeInfos[col].getEstNnz();
				}
			}
		}

		if(LOG.isTraceEnabled()) {
			LOG.trace("C: " + Arrays.toString(colsC.toArray(new Integer[0])));
			LOG.trace(
				"-- compression ratios: " + Arrays.toString(colsC.stream().map(c -> compRatios.get(c)).toArray()));
			LOG.trace("UC: " + Arrays.toString(colsUC.toArray(new Integer[0])));
			LOG.trace(
				"-- compression ratios: " + Arrays.toString(colsUC.stream().map(c -> compRatios.get(c)).toArray()));
		}

		return sizeInfos;
	}

	private static CompressedSizeInfo[] computeCompressedSizeInfos(CompressedSizeEstimator estimator, int clen) {
		CompressedSizeInfo[] ret = new CompressedSizeInfo[clen];
		for(int col = 0; col < clen; col++)
			ret[col] = estimator.estimateCompressedColGroupSize(new int[] {col});
		return ret;
	}

	private static CompressedSizeInfo[] computeCompressedSizeInfos(CompressedSizeEstimator estimator, int clen, int k) {
		try {
			ExecutorService pool = CommonThreadPool.get(k);
			ArrayList<SizeEstimationTask> tasks = new ArrayList<>();
			for(int col = 0; col < clen; col++)
				tasks.add(new SizeEstimationTask(estimator, col));
			List<Future<CompressedSizeInfo>> rtask = pool.invokeAll(tasks);
			ArrayList<CompressedSizeInfo> ret = new ArrayList<>();
			for(Future<CompressedSizeInfo> lrtask : rtask)
				ret.add(lrtask.get());
			pool.shutdown();
			return ret.toArray(new CompressedSizeInfo[0]);
		}
		catch(InterruptedException | ExecutionException e) {
			throw new DMLRuntimeException(e);
		}
	}

	private static class SizeEstimationTask implements Callable<CompressedSizeInfo> {
		private final CompressedSizeEstimator _estimator;
		private final int _col;

		protected SizeEstimationTask(CompressedSizeEstimator estimator, int col) {
			_estimator = estimator;
			_col = col;
		}

		@Override
		public CompressedSizeInfo call() {
			return _estimator.estimateCompressedColGroupSize(new int[] {_col});
		}
	}

	private static Map<CompressionType, Long> calculateCompressionSizes(CompressedSizeEstimationFactors fact) {
		Map<CompressionType, Long> res = new HashMap<>();
		for(CompressionType ct : validCompressions()) {
			res.put(ct, getCompressionSize(ct, fact));
		}
		return res;
	}

	private static CompressionType[] validCompressions() {
		return new CompressionType[] {CompressionType.DDC1, CompressionType.DDC2, CompressionType.OLE_BITMAP,
			CompressionType.RLE_BITMAP};
	}

	private static Long getCompressionSize(CompressionType ct, CompressedSizeEstimationFactors fact) {
		long size = 0;
		// long before = 0;
		switch(ct) {
			case DDC1:
				// before = getDDCSize(fact.numVals, fact.numRows, fact.numCols);
				size = ColGroupSizes.estimateInMemorySizeDDC1(fact.numCols, fact.numVals, fact.numRows, fact.numRows);
				// size = Long.MAX_VALUE;// getDDCSize(fact.numVals, fact.numRows, fact.numCols);
				break;
			case DDC2:
				// before = getDDCSize(fact.numVals, fact.numRows, fact.numCols);
				size = ColGroupSizes.estimateInMemorySizeDDC2(fact.numCols, fact.numVals, fact.numRows, fact.numRows);
				break;
			case RLE_BITMAP:
				// before = getRLESize(fact.numVals, fact.numRuns, fact.numCols);
				size = ColGroupSizes.estimateInMemorySizeRLE(fact.numCols, fact.numVals, fact.numRuns);
				break;
			case OLE_BITMAP:
				if(fact.numVals > 2 * BitmapEncoder.BITMAP_BLOCK_SZ) {
					// Fix Call...
					size = ColGroupSizes
						.estimateInMemorySizeOLE(fact.numCols, fact.numVals, fact.numOffs, fact.numSegs, 0);
				}
				else {
					size = Long.MAX_VALUE;
				}
				// before = getOLESize(fact.numVals, fact.numOffs, fact.numSegs, fact.numCols);
				break;
			case UNCOMPRESSED:
				// before = -1;
				size = ColGroupSizes.estimateInMemorySizeUncompressed(fact.numRows,
					fact.numCols,
					((double) fact.numVals / (fact.numRows * fact.numCols)));
				break;
			// MatrixBlock.estimateSizeInMemory(fact.numRows, fact.numCols, ((double) fact.numVals / (fact.numRows *
			// fact.numCols)));
			default:
				throw new NotImplementedException("The col compression Type is not yet supported");
		}

		// LOG.error( ct + ": " + before + " -- " + size);

		return size;
	}

}
