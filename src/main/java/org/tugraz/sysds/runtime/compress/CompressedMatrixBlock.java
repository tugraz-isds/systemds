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
package org.tugraz.sysds.runtime.compress;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.lops.MMTSJ.MMTSJType;
import org.tugraz.sysds.lops.MapMultChain.ChainType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.compress.cocode.PlanningCoCoder;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroup;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroup.CompressionType;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupCompressor;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupDDC;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupDDC1;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupDDC2;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupOLE;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupOffset;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupRLE;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupUncompressed;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupValue;
import org.tugraz.sysds.runtime.compress.colgroup.DenseRowIterator;
import org.tugraz.sysds.runtime.compress.colgroup.SparseRowIterator;
import org.tugraz.sysds.runtime.compress.estim.CompressedSizeEstimator;
import org.tugraz.sysds.runtime.compress.estim.CompressedSizeEstimatorFactory;
import org.tugraz.sysds.runtime.compress.estim.CompressedSizeInfo;
import org.tugraz.sysds.runtime.compress.utils.ColumnGroupIterator;
import org.tugraz.sysds.runtime.compress.utils.ConverterUtils;
import org.tugraz.sysds.runtime.compress.utils.LinearAlgebraUtils;
import org.tugraz.sysds.runtime.controlprogram.caching.MatrixObject.UpdateType;
import org.tugraz.sysds.runtime.controlprogram.parfor.stat.Timing;
import org.tugraz.sysds.runtime.data.SparseBlock;
import org.tugraz.sysds.runtime.data.SparseRow;
import org.tugraz.sysds.runtime.functionobjects.Builtin;
import org.tugraz.sysds.runtime.functionobjects.Builtin.BuiltinCode;
import org.tugraz.sysds.runtime.functionobjects.KahanFunction;
import org.tugraz.sysds.runtime.functionobjects.KahanPlus;
import org.tugraz.sysds.runtime.functionobjects.KahanPlusSq;
import org.tugraz.sysds.runtime.functionobjects.Multiply;
import org.tugraz.sysds.runtime.functionobjects.ReduceAll;
import org.tugraz.sysds.runtime.functionobjects.ReduceCol;
import org.tugraz.sysds.runtime.instructions.cp.KahanObject;
import org.tugraz.sysds.runtime.matrix.data.IJV;
import org.tugraz.sysds.runtime.matrix.data.LibMatrixBincell;
import org.tugraz.sysds.runtime.matrix.data.LibMatrixReorg;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.data.MatrixIndexes;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue;
import org.tugraz.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.tugraz.sysds.runtime.matrix.operators.AggregateUnaryOperator;
import org.tugraz.sysds.runtime.matrix.operators.BinaryOperator;
import org.tugraz.sysds.runtime.matrix.operators.ScalarOperator;
import org.tugraz.sysds.runtime.util.CommonThreadPool;

public class CompressedMatrixBlock extends AbstractCompressedMatrixBlock {

	// ------------------------------
	// Logging parameters:
	// local debug flag
	private static final boolean LOCAL_DEBUG = false;
	// DEBUG/TRACE for details
	private static final Level LOCAL_DEBUG_LEVEL = Level.DEBUG;

	private static final Log LOG = LogFactory.getLog(CompressedMatrixBlock.class.getName());

	static {
		// for internal debugging only
		if(LOCAL_DEBUG) {
			Logger.getLogger("org.tugraz.sysds.runtime.compress").setLevel(LOCAL_DEBUG_LEVEL);
		}
	}
	// ------------------------------

	// Fields
	// The serialization ID for serializing the block for sending or saving.
	private static final long serialVersionUID = 7319372019143154058L;
	// Transpose input defines if it is allowed to transpose the MatrixBlock such that it compresses row based instead
	// of col based.
	public static final boolean TRANSPOSE_INPUT = true;
	// Threshold for when to parallelize the aggregation functions.
	private static final long MIN_PAR_AGG_THRESHOLD = 16 * 1024 * 1024; // 16MB
	// Specifies if the compression estimates should be investigated
	private static final boolean INVESTIGATE_ESTIMATES = true;
	public static boolean ALLOW_DDC_ENCODING = true;
	public static final boolean ALLOW_SHARED_DDC1_DICTIONARY = true;

	protected CompressionStatistics _stats = null;
	protected boolean _sharedDDC1Dict = false;
	// I the seed is -1 then the system used system millisecond time and class hash for seeding.
	protected long seed = -1;
	// The sampling ratio for the estimator.
	protected double sampling_ratio = 0.05;
	// Sets the compression plan to lossy.
	protected boolean lossy = false;
	// TODO: add precision to the loss of compression

	/**
	 * Constructor for building an empty Compressed Matrix block object.
	 */
	public CompressedMatrixBlock() {
		super();
	}

	/**
	 * Main constructor for building a block from scratch.
	 * 
	 * @param rl     number of rows in the block
	 * @param cl     number of columns
	 * @param sparse true if the UNCOMPRESSED representation of the block should be sparse
	 */
	public CompressedMatrixBlock(int rl, int cl, boolean sparse) {
		super(rl, cl, sparse);
	}

	/**
	 * "Copy" constructor to populate this compressed block with the uncompressed contents of a conventional block. Does
	 * <b>not</b> compress the block. Only creates a shallow copy, and only does deep copy on compression.
	 * 
	 * @param that matrix block
	 */
	public CompressedMatrixBlock(MatrixBlock that) {
		super(that.getNumRows(), that.getNumColumns(), that.isInSparseFormat());

		// shallow copy (deep copy on compression, prevents unnecessary copy)
		if(isInSparseFormat())
			sparseBlock = that.getSparseBlock();
		else
			denseBlock = that.getDenseBlock();
		nonZeros = that.getNonZeros();
	}

	// SETTERS & GETTERS

	public void setSeed(long seed) {
		this.seed = seed;
	}

	public void setSamplingRatio(double sampling_ratio) {
		this.sampling_ratio = sampling_ratio;
	}

	public void setLossy(boolean lossy) {
		this.lossy = true;
	}

	public CompressionStatistics getCompressionStatistics() {
		return _stats;
	}

	public boolean isCompressed() {
		return(_colGroups != null);
	}

	public boolean isSingleUncompressedGroup() {
		return(_colGroups != null && _colGroups.size() == 1 && _colGroups.get(0) instanceof ColGroupUncompressed);
	}

	private void allocateColGroupList() {
		_colGroups = new ArrayList<>();
	}

	/**
	 * A single threaded default execution of the compression.
	 * 
	 * @return a compressed representation of the original matrix block object.
	 */
	public MatrixBlock compress() {
		// Default sequential execution of compression
		return compress(1);
	}

	/**
	 * The main method for compressing the input matrix.
	 * 
	 * SAMPLE-BASED DECISIONS: Decisions such as testing if a column is amenable to bitmap compression or evaluating
	 * co-coding potentials are made based on a subset of the rows. For large data sets, sampling might take a
	 * significant amount of time. So, we generate only one sample and use it for the entire compression process.
	 * 
	 * @param k the number of threads used to execute the compression.
	 * @return A compressed matrix block.
	 */
	public MatrixBlock compress(int k) {
		// check for redundant compression
		if(isCompressed()) {
			throw new DMLRuntimeException("Redundant compression, block already compressed.");
		}

		Timing time = new Timing(true);
		_stats = new CompressionStatistics();

		// prepare basic meta data and deep copy / transpose input
		final int numRows = getNumRows();
		final int numCols = getNumColumns();
		final boolean sparse = isInSparseFormat();

		MatrixBlock rawBlock = !TRANSPOSE_INPUT ? new MatrixBlock(this) : LibMatrixReorg
			.transpose(this, new MatrixBlock(numCols, numRows, sparse), k);

		// construct sample-based size estimator
		CompressedSizeEstimator bitmapSizeEstimator = CompressedSizeEstimatorFactory
			.getSizeEstimator(rawBlock, TRANSPOSE_INPUT, seed, sampling_ratio);

		// PHASE 1: Classify columns by compression type
		// We start by determining which columns are amenable to compression
		List<Integer> colsC = new ArrayList<>();
		List<Integer> colsUC = new ArrayList<>();
		HashMap<Integer, Double> compRatios = new HashMap<>();

		long nnzUC = 0;

		// Classify columns according to ratio (size uncompressed / size compressed),
		// where a column is compressible if ratio > 1.
		CompressedSizeInfo[] sizeInfos = CompressedSizeInfo
			.computeCompressedSizeInfos(bitmapSizeEstimator, colsC, colsUC, compRatios, nnzUC, numRows, numCols, k);

		if(INVESTIGATE_ESTIMATES) {
			_stats.estimatedSizeCols = memoryEstimateCols(sizeInfos, numRows, numCols, colsUC.size(), nnzUC);
		}

		_stats.setNextTimePhase(time.stop());

		if(LOG.isDebugEnabled()) {
			LOG.debug("Compression statistics:");
			LOG.debug("--compression phase 1: " + _stats.getLastTimePhase());
		}

		if(colsC.isEmpty()) {
			LOG.warn("Abort block compression because all columns are incompressible.");
			return new MatrixBlock().copyShallow(this);
		}

		// PHASE 2: Grouping columns
		// Divide the bitmap columns into column groups.
		List<int[]> bitmapColGrps = PlanningCoCoder
			.findCocodesByPartitioning(bitmapSizeEstimator, colsC, sizeInfos, numRows, k);

		_stats.setNextTimePhase(time.stop());

		if(LOG.isDebugEnabled()) {
			LOG.debug("--compression phase 2: " + _stats.getLastTimePhase());
		}

		// if(INVESTIGATE_ESTIMATES) {
		// 	_stats.estimatedSizeColGroups = memoryEstimateColGroups(bitmapColGrps,
		// 		bitmapSizeEstimator,
		// 		numRows,
		// 		numCols,
		// 		colsUC.size(),
		// 		nnzUC);
		// }

		// PHASE 3: Compress and correct sample-based decisions
		ColGroup[] colGroups = ColGroupCompressor
			.compressColGroups(rawBlock, bitmapSizeEstimator, compRatios, numRows, bitmapColGrps, colsUC.isEmpty());

		assignColumns(numCols, colGroups, rawBlock);

		_stats.setNextTimePhase(time.stop());

		if(LOG.isDebugEnabled()) {
			LOG.debug("--compression phase 3: " + _stats.getLastTimePhase());
		}

		// PHASE 4: Best-effort dictionary sharing for DDC1 single-col groups
		double[] dict = createSharedDDC1Dictionary(_colGroups);
		if(dict != null) {
			applySharedDDC1Dictionary(_colGroups, dict);
			_sharedDDC1Dict = true;
		}

		_stats.setNextTimePhase(time.stop());
		if(LOG.isDebugEnabled()) {
			LOG.debug("--compression phase 4: " + _stats.getLastTimePhase());
		}

		// Phase 5: Cleanup
		// The remaining columns are stored uncompressed as one big column group

		_stats.size = estimateCompressedSizeInMemory();
		_stats.originalSize = estimateOriginalSizeInMemory(numRows,numCols, rawBlock.getSparsity());
		_stats.ratio = _stats.originalSize / (double) _stats.size;

		// System.out.print(_stats.ratio);
		if(_stats.ratio < 1) {
			LOG.warn("Abort block compression because compression ratio is less than 1.");
			return new MatrixBlock().copyShallow(this);
		}

		// final cleanup (discard uncompressed block)
		rawBlock.cleanupBlock(true, true);
		this.cleanupBlock(true, true);

		_stats.setNextTimePhase(time.stop());

		_stats.setColGroupsCounts(_colGroups);

		LOG.info("--num col groups: " + _colGroups.size() + ", -- num input cols: " + numCols);
		if(LOG.isDebugEnabled()) {
			LOG.debug("--compression phase 5: " + _stats.getLastTimePhase());
			LOG.debug("--col groups types " + _stats.getGroupsTypesString());
			LOG.debug("--col groups sizes " + _stats.getGroupsSizesString());
			LOG.debug("--compressed size: " + _stats.size);
			LOG.debug("--compression ratio: " + _stats.ratio);
		}

		return this;
	}

	private void assignColumns(int numCols, ColGroup[] colGroups, MatrixBlock rawBlock) {

		allocateColGroupList();
		HashSet<Integer> remainingCols = seq(0, numCols - 1, 1);
		for(int j = 0; j < colGroups.length; j++) {
			if(colGroups[j] != null) {
				for(int col : colGroups[j].getColIndices())
					remainingCols.remove(col);
				_colGroups.add(colGroups[j]);
			}
		}

		if(!remainingCols.isEmpty()) {
			int[] list = remainingCols.stream().mapToInt(i -> i).toArray();
			ColGroupUncompressed ucgroup = new ColGroupUncompressed(list, rawBlock);
			_colGroups.add(ucgroup);
		}
	}

	private static double[] createSharedDDC1Dictionary(List<ColGroup> colGroups) {
		if(!ALLOW_DDC_ENCODING || !ALLOW_SHARED_DDC1_DICTIONARY)
			return null;

		// create joint dictionary
		HashSet<Double> tmp = new HashSet<>();
		int numQual = 0;
		for(ColGroup grp : colGroups)
			if(grp.getNumCols() == 1 && grp instanceof ColGroupDDC1) {
				ColGroupDDC1 grpDDC1 = (ColGroupDDC1) grp;
				for(double val : grpDDC1.getValues())
					tmp.add(val);
				numQual++;
			}

		// abort shared dictionary creation if empty or too large
		int maxSize = tmp.contains(0d) ? 256 : 255;
		if(tmp.isEmpty() || tmp.size() > maxSize || numQual < 2)
			return null;
		LOG.debug("Created shared directionary for " + numQual + " DDC1 single column groups.");

		// build consolidated dictionary
		return tmp.stream().mapToDouble(Double::doubleValue).toArray();
	}

	private static void applySharedDDC1Dictionary(List<ColGroup> colGroups, double[] dict) {
		// create joint mapping table
		HashMap<Double, Integer> map = new HashMap<>();
		for(int i = 0; i < dict.length; i++)
			map.put(dict[i], i);

		// recode data of all relevant DDC1 groups
		for(ColGroup grp : colGroups)
			if(grp.getNumCols() == 1 && grp instanceof ColGroupDDC1) {
				ColGroupDDC1 grpDDC1 = (ColGroupDDC1) grp;
				grpDDC1.recodeData(map);
				grpDDC1.setValues(dict);
			}
	}

	/**
	 * Decompress block.
	 * 
	 * @return a new uncompressed matrix block containing the contents of this block
	 */
	public MatrixBlock decompress() {
		// early abort for not yet compressed blocks
		if(!isCompressed())
			return new MatrixBlock(this);

		Timing time = new Timing(true);

		// preallocation sparse rows to avoid repeated reallocations
		MatrixBlock ret = new MatrixBlock(getNumRows(), getNumColumns(), isInSparseFormat(), getNonZeros());
		if(ret.isInSparseFormat()) {
			int[] rnnz = new int[rlen];
			for(ColGroup grp : _colGroups)
				grp.countNonZerosPerRow(rnnz, 0, rlen);
			ret.allocateSparseRowsBlock();
			SparseBlock rows = ret.getSparseBlock();
			for(int i = 0; i < rlen; i++)
				rows.allocate(i, rnnz[i]);
		}

		// core decompression (append if sparse)
		for(ColGroup grp : _colGroups)
			grp.decompressToBlock(ret, 0, rlen);

		// post-processing (for append in decompress)
		ret.setNonZeros(nonZeros);
		if(ret.isInSparseFormat())
			ret.sortSparseRows();

		if(LOG.isDebugEnabled())
			LOG.debug("decompressed block in " + time.stop() + "ms.");

		return ret;
	}

	/**
	 * Decompress block.
	 * 
	 * @param k degree of parallelism
	 * @return a new uncompressed matrix block containing the contents of this block
	 */
	public MatrixBlock decompress(int k) {
		// early abort for not yet compressed blocks
		if(!isCompressed())
			return new MatrixBlock(this);
		if(k <= 1)
			return decompress();

		Timing time = LOG.isDebugEnabled() ? new Timing(true) : null;

		MatrixBlock ret = new MatrixBlock(rlen, clen, sparse, nonZeros).allocateBlock();

		// multi-threaded decompression
		try {
			ExecutorService pool = CommonThreadPool.get(k);
			int rlen = getNumRows();
			int blklen = BitmapEncoder.getAlignedBlocksize((int) (Math.ceil((double) rlen / k)));
			ArrayList<DecompressTask> tasks = new ArrayList<>();
			for(int i = 0; i < k & i * blklen < getNumRows(); i++)
				tasks.add(new DecompressTask(_colGroups, ret, i * blklen, Math.min((i + 1) * blklen, rlen)));
			List<Future<Object>> rtasks = pool.invokeAll(tasks);
			pool.shutdown();
			for(Future<Object> rt : rtasks)
				rt.get(); // error handling
		}
		catch(Exception ex) {
			throw new DMLRuntimeException(ex);
		}

		// post-processing
		ret.setNonZeros(nonZeros);

		if(LOG.isDebugEnabled())
			LOG.debug("decompressed block w/ k=" + k + " in " + time.stop() + "ms.");

		return ret;
	}

	public long memoryEstimateCols(CompressedSizeInfo[] cols, int numRows, int numCols, int numUncompressedCols,
		long nnzUC) {
		// basic data inherited from MatrixBlock
		long est = MatrixBlock.estimateSizeInMemory(0, 0, 0);
		est += 80 + 8 * (numCols - numUncompressedCols);
		for(CompressedSizeInfo csi : cols) {
			est += csi.getMinSize();
		}
		est += MatrixBlock.estimateSizeInMemory(numRows,
			numUncompressedCols,
			OptimizerUtils.getSparsity(numRows, numUncompressedCols, nnzUC));

		return est;
	}

	// public long memoryEstimateColGroups(List<int[]> bitmapColGrps, CompressedSizeEstimator bitmapSizeEstimator,
	// 	int numRows, int numCols, int numUncompressedCols, long nnzUC) {
	// 	long est = MatrixBlock.estimateSizeInMemory(0, 0, 0);

	// 	est += 80 + 8 * (numCols - numUncompressedCols);

	// 	for(int[] groupIndices : bitmapColGrps)
	// 		est += bitmapSizeEstimator.estimateCompressedColGroupSize(groupIndices).getMinSize();

	// 	// add the size of the incompressable cols.
	// 	est += MatrixBlock.estimateSizeInMemory(numRows,
	// 		numUncompressedCols,
	// 		OptimizerUtils.getSparsity(numRows, numUncompressedCols, nnzUC));

	// 	return est;
	// }

	/**
	 * Obtain an upper bound on the memory used to store the compressed block.
	 * 
	 * @return an upper bound on the memory used to store this compressed block considering class overhead.
	 */
	public long estimateCompressedSizeInMemory() {
		if(!isCompressed())
			return Long.MAX_VALUE;

		// basic data inherited from MatrixBlock
		long total = MatrixBlock.estimateSizeInMemory(0, 0, 0);
		// adding the size of colGroups ArrayList overhead
		// object overhead (32B) + int size (4B) + int modCount (4B) + Object[]
		// elementData overhead + reference (32+8)B +reference ofr each Object (8B)
		total += 80 + 8 * _colGroups.size();
		for(ColGroup grp : _colGroups)
			total += grp.estimateInMemorySize();
		// correction for shared DDC1 dictionary
		if(_sharedDDC1Dict) {
			boolean seenDDC1 = false;
			for(ColGroup grp : _colGroups)
				if(grp.getNumCols() == 1 && grp instanceof ColGroupDDC1) {
					if(seenDDC1)
						total -= ((ColGroupDDC1) grp).getValuesSize();
					seenDDC1 = true;
				}
		}
		return total;
	}


	@Override
	public double quickGetValue(int r, int c) {
		if(!isCompressed()) {
			return super.quickGetValue(r, c);
		}

		// find column group according to col index
		ColGroup grp = null;
		for(ColGroup group : _colGroups)
			if(Arrays.binarySearch(group.getColIndices(), c) >= 0) {
				grp = group;
				break;
			}

		// find row value
		return grp.get(r, c);
	}

	//////////////////////////////////////////
	// Serialization / Deserialization

	@Override
	public long getExactSizeOnDisk() {
		// header information
		long ret = 22;
		for(ColGroup grp : _colGroups) {
			ret += 1; // type info
			ret += grp.getExactSizeOnDisk();
		}
		return ret;
	}

	@Override
	public boolean isShallowSerialize() {
		return false;
	}

	@Override
	public boolean isShallowSerialize(boolean inclConvert) {
		return false;
	}

	@Override
	public void toShallowSerializeBlock() {
		// do nothing
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		boolean compressed = in.readBoolean();

		// deserialize uncompressed block
		if(!compressed) {
			super.readFields(in);
			return;
		}

		// deserialize compressed block
		rlen = in.readInt();
		clen = in.readInt();
		nonZeros = in.readLong();
		_sharedDDC1Dict = in.readBoolean();
		int ncolGroups = in.readInt();

		_colGroups = new ArrayList<>(ncolGroups);
		double[] sharedDict = null;
		for(int i = 0; i < ncolGroups; i++) {
			CompressionType ctype = CompressionType.values()[in.readByte()];
			ColGroup grp = null;

			// create instance of column group
			switch(ctype) {
				case UNCOMPRESSED:
					grp = new ColGroupUncompressed();
					break;
				case OLE_BITMAP:
					grp = new ColGroupOLE();
					break;
				case RLE_BITMAP:
					grp = new ColGroupRLE();
					break;
				case DDC1:
					grp = new ColGroupDDC1();
					break;
				case DDC2:
					grp = new ColGroupDDC2();
					break;
				default:
					throw new DMLRuntimeException("Unsupported ColGroup Type used");
			}

			// deserialize and add column group (flag for shared dictionary passed
			// and numCols evaluated in DDC1 because numCols not available yet
			grp.readFields(in, sharedDict != null);

			// use shared DDC1 dictionary if applicable
			if(_sharedDDC1Dict && grp.getNumCols() == 1 && grp instanceof ColGroupDDC1) {
				if(sharedDict == null)
					sharedDict = ((ColGroupValue) grp).getValues();
				else
					((ColGroupValue) grp).setValues(sharedDict);
			}

			_colGroups.add(grp);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isCompressed());

		// serialize uncompressed block
		if(!isCompressed()) {
			super.write(out);
			return;
		}

		// serialize compressed matrix block
		out.writeInt(rlen);
		out.writeInt(clen);
		out.writeLong(nonZeros);
		out.writeBoolean(_sharedDDC1Dict);
		out.writeInt(_colGroups.size());

		boolean skipDict = false;
		for(ColGroup grp : _colGroups) {
			boolean shared = (grp instanceof ColGroupDDC1 && _sharedDDC1Dict && grp.getNumCols() == 1);
			out.writeByte(grp.getCompType().ordinal());
			grp.write(out, skipDict & shared); // delegate serialization
			skipDict |= shared;
		}
	}

	/**
	 * Redirects the default java serialization via externalizable to our default hadoop writable serialization for
	 * efficient broadcast/rdd deserialization.
	 * 
	 * @param is object input
	 * @throws IOException if IOException occurs
	 */
	@Override
	public void readExternal(ObjectInput is) throws IOException {
		readFields(is);
	}

	/**
	 * Redirects the default java serialization via externalizable to our default hadoop writable serialization for
	 * efficient broadcast/rdd serialization.
	 * 
	 * @param os object output
	 * @throws IOException if IOException occurs
	 */
	@Override
	public void writeExternal(ObjectOutput os) throws IOException {
		write(os);
	}

	public Iterator<IJV> getIterator(int rl, int ru, boolean inclZeros) {
		return getIterator(rl, ru, 0, _colGroups.size(), inclZeros);
	}

	public Iterator<IJV> getIterator(int rl, int ru, int cgl, int cgu, boolean inclZeros) {
		return new ColumnGroupIterator(rl, ru, cgl, cgu, inclZeros, _colGroups);
	}

	public Iterator<double[]> getDenseRowIterator(int rl, int ru) {
		return new DenseRowIterator(rl, ru, _colGroups, clen);
	}

	public Iterator<SparseRow> getSparseRowIterator(int rl, int ru) {
		return new SparseRowIterator(rl, ru, _colGroups, clen);
	}

	public int[] countNonZerosPerRow(int rl, int ru) {
		int[] rnnz = new int[ru - rl];
		for(ColGroup grp : _colGroups)
			grp.countNonZerosPerRow(rnnz, rl, ru);
		return rnnz;
	}

	//////////////////////////////////////////
	// Operations (overwrite existing ops for seamless integration)

	@Override
	public MatrixBlock scalarOperations(ScalarOperator sop, MatrixValue result) {
		// call uncompressed matrix scalar if necessary
		if(!isCompressed()) {
			return super.scalarOperations(sop, result);
		}

		// allocate the output matrix block
		CompressedMatrixBlock ret = null;
		if(result == null || !(result instanceof CompressedMatrixBlock))
			ret = new CompressedMatrixBlock(getNumRows(), getNumColumns(), sparse);
		else {
			ret = (CompressedMatrixBlock) result;
			ret.reset(rlen, clen);
		}

		// Apply the operation recursively to each of the column groups.
		// Most implementations will only modify metadata.
		ArrayList<ColGroup> newColGroups = new ArrayList<>();
		for(ColGroup grp : _colGroups) {
			newColGroups.add(grp.scalarOperation(sop));
		}
		ret._colGroups = newColGroups;
		ret.setNonZeros(rlen * clen);

		return ret;
	}

	@Override
	public MatrixBlock append(MatrixBlock that, MatrixBlock ret) {
		// call uncompressed matrix append if necessary
		if(!isCompressed()) {
			if(that instanceof CompressedMatrixBlock)
				that = ((CompressedMatrixBlock) that).decompress();
			return super.append(that, ret, true);
		}

		final int m = rlen;
		final int n = clen + that.getNumColumns();
		final long nnz = nonZeros + that.getNonZeros();

		// init result matrix
		CompressedMatrixBlock ret2 = null;
		if(ret == null || !(ret instanceof CompressedMatrixBlock)) {
			ret2 = new CompressedMatrixBlock(m, n, isInSparseFormat());
		}
		else {
			ret2 = (CompressedMatrixBlock) ret;
			ret2.reset(m, n);
		}

		// shallow copy of lhs column groups
		ret2.allocateColGroupList();
		ret2._colGroups.addAll(_colGroups);

		// copy of rhs column groups w/ col index shifting
		if(!(that instanceof CompressedMatrixBlock)) {
			that = new CompressedMatrixBlock(that);
			((CompressedMatrixBlock) that).compress();
		}
		List<ColGroup> inColGroups = ((CompressedMatrixBlock) that)._colGroups;
		for(ColGroup group : inColGroups) {
			ColGroup tmp = ConverterUtils.copyColGroup(group);
			tmp.shiftColIndices(clen);
			ret2._colGroups.add(tmp);
		}

		// meta data maintenance
		ret2.setNonZeros(nnz);
		return ret2;
	}

	@Override
	public MatrixBlock chainMatrixMultOperations(MatrixBlock v, MatrixBlock w, MatrixBlock out, ChainType ctype) {
		// call uncompressed matrix mult if necessary
		if(!isCompressed()) {
			return super.chainMatrixMultOperations(v, w, out, ctype);
		}

		if(this.getNumColumns() != v.getNumRows())
			throw new DMLRuntimeException(
				"Dimensions mismatch on mmchain operation (" + this.getNumColumns() + " != " + v.getNumRows() + ")");
		if(v.getNumColumns() != 1)
			throw new DMLRuntimeException(
				"Invalid input vector (column vector expected, but ncol=" + v.getNumColumns() + ")");
		if(w != null && w.getNumColumns() != 1)
			throw new DMLRuntimeException(
				"Invalid weight vector (column vector expected, but ncol=" + w.getNumColumns() + ")");

		// single-threaded mmchain of single uncompressed colgroup
		if(isSingleUncompressedGroup()) {
			return ((ColGroupUncompressed) _colGroups.get(0)).getData().chainMatrixMultOperations(v, w, out, ctype);
		}

		// Timing time = new Timing(true);

		// prepare result
		if(out != null)
			out.reset(clen, 1, false);
		else
			out = new MatrixBlock(clen, 1, false);

		// empty block handling
		if(isEmptyBlock(false))
			return out;

		// compute matrix mult
		MatrixBlock tmp = new MatrixBlock(rlen, 1, false);
		rightMultByVector(v, tmp);
		if(ctype == ChainType.XtwXv) {
			BinaryOperator bop = new BinaryOperator(Multiply.getMultiplyFnObject());
			LibMatrixBincell.bincellOpInPlace(tmp, w, bop);
		}
		leftMultByVectorTranspose(_colGroups, tmp, out, true, true);

		return out;
	}

	@Override
	public MatrixBlock chainMatrixMultOperations(MatrixBlock v, MatrixBlock w, MatrixBlock out, ChainType ctype,
		int k) {
		// call uncompressed matrix mult if necessary
		if(!isCompressed()) {
			return super.chainMatrixMultOperations(v, w, out, ctype, k);
		}

		if(this.getNumColumns() != v.getNumRows())
			throw new DMLRuntimeException(
				"Dimensions mismatch on mmchain operation (" + this.getNumColumns() + " != " + v.getNumRows() + ")");
		if(v.getNumColumns() != 1)
			throw new DMLRuntimeException(
				"Invalid input vector (column vector expected, but ncol=" + v.getNumColumns() + ")");
		if(w != null && w.getNumColumns() != 1)
			throw new DMLRuntimeException(
				"Invalid weight vector (column vector expected, but ncol=" + w.getNumColumns() + ")");

		// multi-threaded mmchain of single uncompressed colgroup
		if(isSingleUncompressedGroup()) {
			return ((ColGroupUncompressed) _colGroups.get(0)).getData().chainMatrixMultOperations(v, w, out, ctype, k);
		}

		// Timing time = LOG.isDebugEnabled() ? new Timing(true) : null;

		// prepare result
		if(out != null)
			out.reset(clen, 1, false);
		else
			out = new MatrixBlock(clen, 1, false);

		// empty block handling
		if(isEmptyBlock(false))
			return out;

		// compute matrix mult
		MatrixBlock tmp = new MatrixBlock(rlen, 1, false);
		rightMultByVector(v, tmp, k);
		if(ctype == ChainType.XtwXv) {
			BinaryOperator bop = new BinaryOperator(Multiply.getMultiplyFnObject());
			LibMatrixBincell.bincellOpInPlace(tmp, w, bop);
		}
		leftMultByVectorTranspose(_colGroups, tmp, out, true, k);

		// if(LOG.isDebugEnabled())
		// LOG.debug("Compressed MMChain k=" + k + " in " + time.stop());

		return out;
	}

	@Override
	public MatrixBlock aggregateBinaryOperations(MatrixBlock m1, MatrixBlock m2, MatrixBlock ret,
		AggregateBinaryOperator op) {
		// call uncompressed matrix mult if necessary
		if(!isCompressed()) {
			return super.aggregateBinaryOperations(m1, m2, ret, op);
		}

		// multi-threaded mm of single uncompressed colgroup
		if(isSingleUncompressedGroup()) {
			MatrixBlock tmp = ((ColGroupUncompressed) _colGroups.get(0)).getData();
			return tmp.aggregateBinaryOperations(this == m1 ? tmp : m1, this == m2 ? tmp : m2, ret, op);
		}

		Timing time = LOG.isDebugEnabled() ? new Timing(true) : null;

		// setup meta data (dimensions, sparsity)
		int rl = m1.getNumRows();
		int cl = m2.getNumColumns();

		// create output matrix block
		if(ret == null)
			ret = new MatrixBlock(rl, cl, false, rl * cl);
		else
			ret.reset(rl, cl, false, rl * cl);

		// compute matrix mult
		if(m1.getNumRows() > 1 && m2.getNumColumns() == 1) { // MV right
			CompressedMatrixBlock cmb = (CompressedMatrixBlock) m1;
			if(op.getNumThreads() > 1)
				cmb.rightMultByVector(m2, ret, op.getNumThreads());
			else
				cmb.rightMultByVector(m2, ret);
		}
		else if(m1.getNumRows() == 1 && m2.getNumColumns() > 1) { // MV left
			if(op.getNumThreads() > 1)
				leftMultByVectorTranspose(_colGroups, m1, ret, false, op.getNumThreads());
			else
				leftMultByVectorTranspose(_colGroups, m1, ret, false, true);
		}
		else { // MM
				// prepare the other input (including decompression if necessary)
			boolean right = (m1 == this);
			MatrixBlock that = right ? m2 : m1;
			if(that instanceof CompressedMatrixBlock) {
				that = ((CompressedMatrixBlock) that).isCompressed() ? ((CompressedMatrixBlock) that)
					.decompress() : that;
			}

			// transpose for sequential repeated column access
			if(right) {
				that = LibMatrixReorg.transpose(that,
					new MatrixBlock(that.getNumColumns(), that.getNumRows(), that.isInSparseFormat()),
					op.getNumThreads());
			}

			MatrixBlock tmpIn = new MatrixBlock(1, that.getNumColumns(), false).allocateBlock();
			MatrixBlock tmpOut = new MatrixBlock(right ? rl : 1, right ? 1 : cl, false).allocateBlock();
			if(right) { // MM right
				for(int i = 0; i < that.getNumRows(); i++) { // on transpose
					tmpIn = that.slice(i, i, 0, that.getNumColumns() - 1, tmpIn);
					MatrixBlock tmpIn2 = LibMatrixReorg.transpose(tmpIn, // meta data op
						new MatrixBlock(tmpIn.getNumColumns(), tmpIn.getNumRows(), false));
					tmpOut.reset(tmpOut.getNumRows(), tmpOut.getNumColumns());
					if(op.getNumThreads() > 1)
						rightMultByVector(tmpIn2, tmpOut, op.getNumThreads());
					else
						rightMultByVector(tmpIn2, tmpOut);
					ret.leftIndexingOperations(tmpOut, 0, ret.getNumRows() - 1, i, i, ret, UpdateType.INPLACE);
				}
			}
			else { // MM left
				for(int i = 0; i < that.getNumRows(); i++) {
					tmpIn = that.slice(i, i, 0, that.getNumColumns() - 1, tmpIn);
					if(op.getNumThreads() > 1)
						leftMultByVectorTranspose(_colGroups, tmpIn, tmpOut, false, op.getNumThreads());
					else
						leftMultByVectorTranspose(_colGroups, tmpIn, tmpOut, false, true);
					ret.leftIndexingOperations(tmpOut, i, i, 0, ret.getNumColumns() - 1, ret, UpdateType.INPLACE);
				}
			}
		}

		if(LOG.isDebugEnabled())
			LOG.debug("Compressed MM in " + time.stop());

		return ret;
	}

	@Override
	public MatrixBlock aggregateUnaryOperations(AggregateUnaryOperator op, MatrixValue result, int blen,
		MatrixIndexes indexesIn, boolean inCP) {
		// call uncompressed matrix mult if necessary
		if(!isCompressed()) {
			return super.aggregateUnaryOperations(op, result, blen, indexesIn, inCP);
		}

		// check for supported operations
		if(!(op.aggOp.increOp.fn instanceof KahanPlus || op.aggOp.increOp.fn instanceof KahanPlusSq ||
			(op.aggOp.increOp.fn instanceof Builtin &&
				(((Builtin) op.aggOp.increOp.fn).getBuiltinCode() == BuiltinCode.MIN ||
					((Builtin) op.aggOp.increOp.fn).getBuiltinCode() == BuiltinCode.MAX)))) {
			throw new DMLRuntimeException("Unary aggregates other than sum/sumsq/min/max not supported yet.");
		}

		Timing time = LOG.isDebugEnabled() ? new Timing(true) : null;

		// prepare output dimensions
		CellIndex tempCellIndex = new CellIndex(-1, -1);
		op.indexFn.computeDimension(rlen, clen, tempCellIndex);
		// Correction no long exists
		if(op.aggOp.existsCorrection()) {
			switch(op.aggOp.correction) {
				case LASTROW:
					tempCellIndex.row++;
					break;
				case LASTCOLUMN:
					tempCellIndex.column++;
					break;
				case LASTTWOROWS:
					tempCellIndex.row += 2;
					break;
				case LASTTWOCOLUMNS:
					tempCellIndex.column += 2;
					break;
				default:
					throw new DMLRuntimeException("unrecognized correctionLocation: " + op.aggOp.correction);
			}
		}

		// initialize and allocate the result
		if(result == null)
			result = new MatrixBlock(tempCellIndex.row, tempCellIndex.column, false);
		else
			result.reset(tempCellIndex.row, tempCellIndex.column, false);
		MatrixBlock ret = (MatrixBlock) result;
		ret.allocateDenseBlock();

		// special handling init value for rowmins/rowmax
		if(op.indexFn instanceof ReduceCol && op.aggOp.increOp.fn instanceof Builtin) {
			double val = (((Builtin) op.aggOp.increOp.fn)
				.getBuiltinCode() == BuiltinCode.MAX) ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
			ret.getDenseBlock().set(val);
		}

		// core unary aggregate
		if(op.getNumThreads() > 1 && getExactSizeOnDisk() > MIN_PAR_AGG_THRESHOLD) {
			// multi-threaded execution of all groups
			ArrayList<ColGroup>[] grpParts = createStaticTaskPartitioning(
				(op.indexFn instanceof ReduceCol) ? 1 : op.getNumThreads(),
				false);
			ColGroupUncompressed uc = getUncompressedColGroup();
			try {
				// compute uncompressed column group in parallel (otherwise bottleneck)
				if(uc != null)
					uc.unaryAggregateOperations(op, ret);
				// compute all compressed column groups
				ExecutorService pool = CommonThreadPool.get(op.getNumThreads());
				ArrayList<UnaryAggregateTask> tasks = new ArrayList<>();
				if(op.indexFn instanceof ReduceCol && grpParts.length > 0) {
					int blklen = BitmapEncoder
						.getAlignedBlocksize((int) (Math.ceil((double) rlen / op.getNumThreads())));
					for(int i = 0; i < op.getNumThreads() & i * blklen < rlen; i++)
						tasks.add(
							new UnaryAggregateTask(grpParts[0], ret, i * blklen, Math.min((i + 1) * blklen, rlen), op));
				}
				else
					for(ArrayList<ColGroup> grp : grpParts)
						tasks.add(new UnaryAggregateTask(grp, ret, 0, rlen, op));
				List<Future<MatrixBlock>> rtasks = pool.invokeAll(tasks);
				pool.shutdown();

				// aggregate partial results
				if(op.indexFn instanceof ReduceAll) {
					if(op.aggOp.increOp.fn instanceof KahanFunction) {
						KahanObject kbuff = new KahanObject(ret.quickGetValue(0, 0), 0);
						for(Future<MatrixBlock> rtask : rtasks) {
							double tmp = rtask.get().quickGetValue(0, 0);
							((KahanFunction) op.aggOp.increOp.fn).execute2(kbuff, tmp);
						}
						ret.quickSetValue(0, 0, kbuff._sum);
					}
					else {
						double val = ret.quickGetValue(0, 0);
						for(Future<MatrixBlock> rtask : rtasks) {
							double tmp = rtask.get().quickGetValue(0, 0);
							val = op.aggOp.increOp.fn.execute(val, tmp);
						}
						ret.quickSetValue(0, 0, val);
					}
				}
			}
			catch(Exception ex) {
				throw new DMLRuntimeException(ex);
			}
		}
		else {
			// process UC column group
			for(ColGroup grp : _colGroups)
				if(grp instanceof ColGroupUncompressed)
					grp.unaryAggregateOperations(op, ret);

			// process OLE/RLE column groups
			aggregateUnaryOperations(op, _colGroups, ret, 0, rlen);
		}

		// special handling zeros for rowmins/rowmax
		if(op.indexFn instanceof ReduceCol && op.aggOp.increOp.fn instanceof Builtin) {
			int[] rnnz = new int[rlen];
			for(ColGroup grp : _colGroups)
				grp.countNonZerosPerRow(rnnz, 0, rlen);
			Builtin builtin = (Builtin) op.aggOp.increOp.fn;
			for(int i = 0; i < rlen; i++)
				if(rnnz[i] < clen)
					ret.quickSetValue(i, 0, builtin.execute(ret.quickGetValue(i, 0), 0));
		}

		// drop correction if necessary
		if(op.aggOp.existsCorrection() && inCP)
			ret.dropLastRowsOrColumns(op.aggOp.correction);

		// post-processing
		ret.recomputeNonZeros();

		if(LOG.isDebugEnabled())
			LOG.debug("Compressed uagg k=" + op.getNumThreads() + " in " + time.stop());

		return ret;
	}

	@Override
	public MatrixBlock aggregateUnaryOperations(AggregateUnaryOperator op, MatrixValue result, int blen,
		MatrixIndexes indexesIn) {
		return aggregateUnaryOperations(op, result, blen, indexesIn, false);
	}

	private static void aggregateUnaryOperations(AggregateUnaryOperator op, List<ColGroup> groups, MatrixBlock ret,
		int rl, int ru) {
		boolean cacheDDC1 = ColGroupValue.LOW_LEVEL_OPT && op.indexFn instanceof ReduceCol &&
			op.aggOp.increOp.fn instanceof KahanPlus // rowSums
			&& ColGroupOffset.ALLOW_CACHE_CONSCIOUS_ROWSUMS && ru - rl > ColGroupOffset.WRITE_CACHE_BLKSZ / 2;

		// process cache-conscious DDC1 groups (adds to output)
		if(cacheDDC1) {
			ArrayList<ColGroupDDC1> tmp = new ArrayList<>();
			for(ColGroup grp : groups)
				if(grp instanceof ColGroupDDC1)
					tmp.add((ColGroupDDC1) grp);
			if(!tmp.isEmpty())
				ColGroupDDC1
					.computeRowSums(tmp.toArray(new ColGroupDDC1[0]), ret, KahanPlus.getKahanPlusFnObject(), rl, ru);
		}

		// process remaining groups (adds to output)
		// note: UC group never passed into this function
		for(ColGroup grp : groups)
			if(!(grp instanceof ColGroupUncompressed) && !(cacheDDC1 && grp instanceof ColGroupDDC1))
				((ColGroupValue) grp).unaryAggregateOperations(op, ret, rl, ru);
	}

	@Override
	public MatrixBlock transposeSelfMatrixMultOperations(MatrixBlock out, MMTSJType tstype) {
		// call uncompressed matrix mult if necessary
		if(!isCompressed()) {
			return super.transposeSelfMatrixMultOperations(out, tstype);
		}

		// single-threaded tsmm of single uncompressed colgroup
		if(isSingleUncompressedGroup()) {
			return ((ColGroupUncompressed) _colGroups.get(0)).getData().transposeSelfMatrixMultOperations(out, tstype);
		}

		Timing time = LOG.isDebugEnabled() ? new Timing(true) : null;

		// check for transpose type
		if(tstype != MMTSJType.LEFT) // right not supported yet
			throw new DMLRuntimeException("Invalid MMTSJ type '" + tstype.toString() + "'.");

		// create output matrix block
		if(out == null)
			out = new MatrixBlock(clen, clen, false);
		else
			out.reset(clen, clen, false);
		out.allocateDenseBlock();

		if(!isEmptyBlock(false)) {
			// compute matrix mult
			leftMultByTransposeSelf(_colGroups, out, 0, _colGroups.size());

			// post-processing
			out.setNonZeros(LinearAlgebraUtils.copyUpperToLowerTriangle(out));
		}

		if(LOG.isDebugEnabled())
			LOG.debug("Compressed TSMM in " + time.stop());

		return out;
	}

	@Override
	public MatrixBlock transposeSelfMatrixMultOperations(MatrixBlock out, MMTSJType tstype, int k) {
		// call uncompressed matrix mult if necessary
		if(!isCompressed()) {
			return super.transposeSelfMatrixMultOperations(out, tstype, k);
		}

		// multi-threaded tsmm of single uncompressed colgroup
		if(isSingleUncompressedGroup()) {
			return ((ColGroupUncompressed) _colGroups.get(0)).getData()
				.transposeSelfMatrixMultOperations(out, tstype, k);
		}

		Timing time = LOG.isDebugEnabled() ? new Timing(true) : null;

		// check for transpose type
		if(tstype != MMTSJType.LEFT) // right not supported yet
			throw new DMLRuntimeException("Invalid MMTSJ type '" + tstype.toString() + "'.");

		// create output matrix block
		if(out == null)
			out = new MatrixBlock(clen, clen, false);
		else
			out.reset(clen, clen, false);
		out.allocateDenseBlock();

		if(!isEmptyBlock(false)) {
			// compute matrix mult
			try {
				ExecutorService pool = CommonThreadPool.get(k);
				ArrayList<MatrixMultTransposeTask> tasks = new ArrayList<>();
				int numgrp = _colGroups.size();
				int blklen = (int) (Math.ceil((double) numgrp / (2 * k)));
				for(int i = 0; i < 2 * k & i * blklen < clen; i++)
					tasks.add(
						new MatrixMultTransposeTask(_colGroups, out, i * blklen, Math.min((i + 1) * blklen, numgrp)));
				List<Future<Object>> ret = pool.invokeAll(tasks);
				for(Future<Object> tret : ret)
					tret.get(); // check for errors
				pool.shutdown();
			}
			catch(InterruptedException | ExecutionException e) {
				LOG.error(e.getMessage());
				throw new DMLRuntimeException(e);
			}

			// post-processing
			out.setNonZeros(LinearAlgebraUtils.copyUpperToLowerTriangle(out));
		}

		if(LOG.isDebugEnabled())
			LOG.debug("Compressed TSMM k=" + k + " in " + time.stop());

		return out;
	}

	/**
	 * Multiply this matrix block by a column vector on the right.
	 * 
	 * @param vector right-hand operand of the multiplication
	 * @param result buffer to hold the result; must have the appropriate size already
	 */
	private void rightMultByVector(MatrixBlock vector, MatrixBlock result) {
		// initialize and allocate the result
		result.allocateDenseBlock();

		// delegate matrix-vector operation to each column group
		rightMultByVector(_colGroups, vector, result, true, 0, result.getNumRows());

		// post-processing
		result.recomputeNonZeros();
	}

	/**
	 * Multi-threaded version of rightMultByVector.
	 * 
	 * @param vector matrix block vector
	 * @param result matrix block result
	 * @param k      number of threads
	 */
	private void rightMultByVector(MatrixBlock vector, MatrixBlock result, int k) {
		// initialize and allocate the result
		result.allocateDenseBlock();

		// multi-threaded execution of all groups
		try {
			ColGroupUncompressed uc = getUncompressedColGroup();

			// compute uncompressed column group in parallel
			if(uc != null)
				uc.rightMultByVector(vector, result, k);

			// compute remaining compressed column groups in parallel
			ExecutorService pool = CommonThreadPool.get(k);
			int rlen = getNumRows();
			int blklen = BitmapEncoder.getAlignedBlocksize((int) (Math.ceil((double) rlen / k)));
			ArrayList<RightMatrixMultTask> tasks = new ArrayList<>();
			for(int i = 0; i < k & i * blklen < getNumRows(); i++)
				tasks.add(
					new RightMatrixMultTask(_colGroups, vector, result, i * blklen, Math.min((i + 1) * blklen, rlen)));
			List<Future<Long>> ret = pool.invokeAll(tasks);
			pool.shutdown();

			// error handling and nnz aggregation
			long lnnz = 0;
			for(Future<Long> tmp : ret)
				lnnz += tmp.get();
			result.setNonZeros(lnnz);
		}
		catch(Exception ex) {
			throw new DMLRuntimeException(ex);
		}
	}

	private static void rightMultByVector(List<ColGroup> groups, MatrixBlock vect, MatrixBlock ret, boolean inclUC,
		int rl, int ru) {
		ColGroupValue.setupThreadLocalMemory(getMaxNumValues(groups));

		boolean cacheDDC1 = ColGroupValue.LOW_LEVEL_OPT && ru - rl > ColGroupOffset.WRITE_CACHE_BLKSZ;

		// process uncompressed column group (overwrites output)
		if(inclUC) {
			for(ColGroup grp : groups)
				if(grp instanceof ColGroupUncompressed)
					grp.rightMultByVector(vect, ret, rl, ru);
		}

		// process cache-conscious DDC1 groups (adds to output)
		if(cacheDDC1) {
			ArrayList<ColGroupDDC1> tmp = new ArrayList<>();
			for(ColGroup grp : groups)
				if(grp instanceof ColGroupDDC1)
					tmp.add((ColGroupDDC1) grp);
			if(!tmp.isEmpty())
				ColGroupDDC1.rightMultByVector(tmp.toArray(new ColGroupDDC1[0]), vect, ret, rl, ru);
		}

		// process remaining groups (adds to output)
		for(ColGroup grp : groups)
			if(!(grp instanceof ColGroupUncompressed) && !(cacheDDC1 && grp instanceof ColGroupDDC1))
				grp.rightMultByVector(vect, ret, rl, ru);

		ColGroupValue.cleanupThreadLocalMemory();
	}

	/**
	 * Multiply this matrix block by the transpose of a column vector (i.e. t(v)%*%X)
	 * 
	 * @param colGroups   list of column groups
	 * @param vector      left-hand operand of the multiplication
	 * @param result      buffer to hold the result; must have the appropriate size already
	 * @param doTranspose if true, transpose vector
	 */
	private static void leftMultByVectorTranspose(List<ColGroup> colGroups, MatrixBlock vector, MatrixBlock result,
		boolean doTranspose, boolean allocTmp) {
		// transpose vector if required
		MatrixBlock rowVector = vector;
		if(doTranspose) {
			rowVector = new MatrixBlock(1, vector.getNumRows(), false);
			LibMatrixReorg.transpose(vector, rowVector);
		}

		// initialize and allocate the result
		result.reset();
		result.allocateDenseBlock();

		// setup memory pool for reuse
		if(allocTmp)
			ColGroupValue.setupThreadLocalMemory(getMaxNumValues(colGroups));

		// delegate matrix-vector operation to each column group
		for(ColGroup grp : colGroups) {
			grp.leftMultByRowVector(rowVector, result);
		}

		// post-processing
		if(allocTmp)
			ColGroupValue.cleanupThreadLocalMemory();
		result.recomputeNonZeros();
	}

	private static void leftMultByVectorTranspose(List<ColGroup> colGroups, ColGroupDDC vector, MatrixBlock result) {
		// initialize and allocate the result
		result.reset();
		// delegate matrix-vector operation to each column group
		for(ColGroup grp : colGroups)
			((ColGroupValue) grp).leftMultByRowVector(vector, result);
		// post-processing
		result.recomputeNonZeros();
	}

	/**
	 * Multi-thread version of leftMultByVectorTranspose.
	 * 
	 * @param colGroups   list of column groups
	 * @param vector      left-hand operand of the multiplication
	 * @param result      buffer to hold the result; must have the appropriate size already
	 * @param doTranspose if true, transpose vector
	 * @param k           number of threads
	 */
	private void leftMultByVectorTranspose(List<ColGroup> colGroups, MatrixBlock vector, MatrixBlock result,
		boolean doTranspose, int k) {
		// transpose vector if required
		MatrixBlock rowVector = vector;
		if(doTranspose) {
			rowVector = new MatrixBlock(1, vector.getNumRows(), false);
			LibMatrixReorg.transpose(vector, rowVector);
		}

		// initialize and allocate the result
		result.reset();
		result.allocateDenseBlock();

		// multi-threaded execution
		try {
			// compute uncompressed column group in parallel
			ColGroupUncompressed uc = getUncompressedColGroup();
			if(uc != null)
				uc.leftMultByRowVector(rowVector, result, k);

			// compute remaining compressed column groups in parallel
			ExecutorService pool = CommonThreadPool.get(Math.min(colGroups.size() - ((uc != null) ? 1 : 0), k));
			ArrayList<ColGroup>[] grpParts = createStaticTaskPartitioning(4 * k, false);
			ArrayList<LeftMatrixMultTask> tasks = new ArrayList<>();
			for(ArrayList<ColGroup> groups : grpParts)
				tasks.add(new LeftMatrixMultTask(groups, rowVector, result));
			List<Future<Object>> ret = pool.invokeAll(tasks);
			pool.shutdown();
			for(Future<Object> tmp : ret)
				tmp.get(); // error handling
		}
		catch(Exception ex) {
			throw new DMLRuntimeException(ex);
		}

		// post-processing
		result.recomputeNonZeros();
	}

	private static void leftMultByTransposeSelf(List<ColGroup> groups, MatrixBlock result, int gl, int gu) {
		final int numRows = groups.get(0).getNumRows();
		final int numGroups = groups.size();
		final boolean containsUC = containsUncompressedColGroup(groups);

		// preallocated dense tmp matrix blocks
		MatrixBlock lhs = new MatrixBlock(1, numRows, false);
		MatrixBlock tmpret = new MatrixBlock(1, result.getNumColumns(), false);
		lhs.allocateDenseBlock();
		tmpret.allocateDenseBlock();

		// setup memory pool for reuse
		ColGroupValue.setupThreadLocalMemory(getMaxNumValues(groups));

		// approach: for each colgroup, extract uncompressed columns one at-a-time
		// vector-matrix multiplies against remaining col groups
		for(int i = gl; i < gu; i++) {
			// get current group and relevant col groups
			ColGroup group = groups.get(i);
			int[] ixgroup = group.getColIndices();
			List<ColGroup> tmpList = groups.subList(i, numGroups);

			if(group instanceof ColGroupDDC // single DDC group
				&& ixgroup.length == 1 && !containsUC && numRows < BitmapEncoder.BITMAP_BLOCK_SZ) {
				// compute vector-matrix partial result
				leftMultByVectorTranspose(tmpList, (ColGroupDDC) group, tmpret);

				// write partial results (disjoint non-zeros)
				LinearAlgebraUtils.copyNonZerosToUpperTriangle(result, tmpret, ixgroup[0]);
			}
			else {
				// for all uncompressed lhs columns vectors
				for(int j = 0; j < ixgroup.length; j++) {
					group.decompressToBlock(lhs, j);

					if(!lhs.isEmptyBlock(false)) {
						// compute vector-matrix partial result
						leftMultByVectorTranspose(tmpList, lhs, tmpret, false, false);

						// write partial results (disjoint non-zeros)
						LinearAlgebraUtils.copyNonZerosToUpperTriangle(result, tmpret, ixgroup[j]);
					}
				}
			}
		}

		// post processing
		ColGroupValue.cleanupThreadLocalMemory();
	}

	@SuppressWarnings("unchecked")
	private ArrayList<ColGroup>[] createStaticTaskPartitioning(int k, boolean inclUncompressed) {
		// special case: single uncompressed col group
		if(_colGroups.size() == 1 && _colGroups.get(0) instanceof ColGroupUncompressed) {
			return new ArrayList[0];
		}

		// initialize round robin col group distribution
		// (static task partitioning to reduce mem requirements/final agg)
		int numTasks = Math.min(k, _colGroups.size());
		ArrayList<ColGroup>[] grpParts = new ArrayList[numTasks];
		int pos = 0;
		for(ColGroup grp : _colGroups) {
			if(grpParts[pos] == null)
				grpParts[pos] = new ArrayList<>();
			if(inclUncompressed || !(grp instanceof ColGroupUncompressed)) {
				grpParts[pos].add(grp);
				pos = (pos == numTasks - 1) ? 0 : pos + 1;
			}
		}

		return grpParts;
	}

	private static int getMaxNumValues(List<ColGroup> groups) {
		int numVals = 1;
		for(ColGroup grp : groups)
			if(grp instanceof ColGroupValue)
				numVals = Math.max(numVals, ((ColGroupValue) grp).getNumValues());
		return numVals;
	}

	public boolean hasUncompressedColGroup() {
		return getUncompressedColGroup() != null;
	}

	private ColGroupUncompressed getUncompressedColGroup() {
		for(ColGroup grp : _colGroups)
			if(grp instanceof ColGroupUncompressed)
				return (ColGroupUncompressed) grp;

		return null;
	}

	private static boolean containsUncompressedColGroup(List<ColGroup> groups) {
		for(ColGroup grp : groups)
			if(grp instanceof ColGroupUncompressed)
				return true;
		return false;
	}

	private static class LeftMatrixMultTask implements Callable<Object> {
		private final ArrayList<ColGroup> _groups;
		private final MatrixBlock _vect;
		private final MatrixBlock _ret;

		protected LeftMatrixMultTask(ArrayList<ColGroup> groups, MatrixBlock vect, MatrixBlock ret) {
			_groups = groups;
			_vect = vect;
			_ret = ret;
		}

		@Override
		public Object call() {
			// setup memory pool for reuse
			ColGroupValue.setupThreadLocalMemory(getMaxNumValues(_groups));

			// delegate matrix-vector operation to each column group
			for(ColGroup grp : _groups)
				grp.leftMultByRowVector(_vect, _ret);

			ColGroupValue.cleanupThreadLocalMemory();
			return null;
		}
	}

	private static class RightMatrixMultTask implements Callable<Long> {
		private final List<ColGroup> _groups;
		private final MatrixBlock _vect;
		private final MatrixBlock _ret;
		private final int _rl;
		private final int _ru;

		protected RightMatrixMultTask(List<ColGroup> groups, MatrixBlock vect, MatrixBlock ret, int rl, int ru) {
			_groups = groups;
			_vect = vect;
			_ret = ret;
			_rl = rl;
			_ru = ru;
		}

		@Override
		public Long call() {
			rightMultByVector(_groups, _vect, _ret, false, _rl, _ru);
			return _ret.recomputeNonZeros(_rl, _ru - 1, 0, 0);
		}
	}

	private static class MatrixMultTransposeTask implements Callable<Object> {
		private final List<ColGroup> _groups;
		private final MatrixBlock _ret;
		private final int _gl;
		private final int _gu;

		protected MatrixMultTransposeTask(List<ColGroup> groups, MatrixBlock ret, int gl, int gu) {
			_groups = groups;
			_ret = ret;
			_gl = gl;
			_gu = gu;
		}

		@Override
		public Object call() {
			leftMultByTransposeSelf(_groups, _ret, _gl, _gu);
			return null;
		}
	}

	private static class UnaryAggregateTask implements Callable<MatrixBlock> {
		private final List<ColGroup> _groups;
		private final int _rl;
		private final int _ru;
		private final MatrixBlock _ret;
		private final AggregateUnaryOperator _op;

		protected UnaryAggregateTask(List<ColGroup> groups, MatrixBlock ret, int rl, int ru,
			AggregateUnaryOperator op) {
			_groups = groups;
			_op = op;
			_rl = rl;
			_ru = ru;

			if(_op.indexFn instanceof ReduceAll) { // sum
				_ret = new MatrixBlock(ret.getNumRows(), ret.getNumColumns(), false);
				_ret.allocateDenseBlock();
				if(_op.aggOp.increOp.fn instanceof Builtin)
					System.arraycopy(ret.getDenseBlockValues(),
						0,
						_ret.getDenseBlockValues(),
						0,
						ret.getNumRows() * ret.getNumColumns());
			}
			else { // colSums
				_ret = ret;
			}
		}

		@Override
		public MatrixBlock call() {
			aggregateUnaryOperations(_op, _groups, _ret, _rl, _ru);
			return _ret;
		}
	}

	private static class DecompressTask implements Callable<Object> {
		private final List<ColGroup> _colGroups;
		private final MatrixBlock _ret;
		private final int _rl;
		private final int _ru;

		protected DecompressTask(List<ColGroup> colGroups, MatrixBlock ret, int rl, int ru) {
			_colGroups = colGroups;
			_ret = ret;
			_rl = rl;
			_ru = ru;
		}

		@Override
		public Object call() {

			// preallocate sparse rows to avoid repeated alloc
			if(_ret.isInSparseFormat()) {
				int[] rnnz = new int[_ru - _rl];
				for(ColGroup grp : _colGroups)
					grp.countNonZerosPerRow(rnnz, _rl, _ru);
				SparseBlock rows = _ret.getSparseBlock();
				for(int i = _rl; i < _ru; i++)
					rows.allocate(i, rnnz[i - _rl]);
			}

			// decompress row partition
			for(ColGroup grp : _colGroups)
				grp.decompressToBlock(_ret, _rl, _ru);

			// post processing (sort due to append)
			if(_ret.isInSparseFormat())
				_ret.sortSparseRows(_rl, _ru);

			return null;
		}
	}

	private static HashSet<Integer> seq(int from, int to, int incr) {
		HashSet<Integer> ret = new HashSet<>();
		for(int i = from; i <= to; i += incr)
			ret.add(i);
		return ret;
	}

}