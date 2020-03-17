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

package org.tugraz.sysds.runtime.compress.colgroup;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.tugraz.sysds.runtime.compress.UncompressedBitmap;
import org.tugraz.sysds.runtime.compress.utils.ConverterUtils;
import org.tugraz.sysds.runtime.data.DenseBlock;
import org.tugraz.sysds.runtime.functionobjects.KahanFunction;
import org.tugraz.sysds.runtime.functionobjects.KahanPlus;
import org.tugraz.sysds.runtime.instructions.cp.KahanObject;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.operators.ScalarOperator;

/**
 * Class to encapsulate information about a column group that is encoded with dense dictionary encoding (DDC) using 1
 * byte codes.
 */
public class ColGroupDDC1 extends ColGroupDDC {
	private static final long serialVersionUID = 5204955589230760157L;

	private byte[] _data;

	public ColGroupDDC1() {
		super();
	}

	public ColGroupDDC1(int[] colIndices, int numRows, UncompressedBitmap ubm) {
		super(colIndices, numRows, ubm);
		_data = new byte[numRows];

		int numVals = ubm.getNumValues();
		int numCols = ubm.getNumColumns();

		// materialize zero values, if necessary
		if(ubm.getNumOffsets() < (long) numRows * numCols) {
			int zeroIx = containsAllZeroValue();
			if(zeroIx < 0) {
				zeroIx = numVals;
				_values = Arrays.copyOf(_values, _values.length + numCols);
			}
			Arrays.fill(_data, (byte) zeroIx);
		}

		// iterate over values and write dictionary codes
		for(int i = 0; i < numVals; i++) {
			int[] tmpList = ubm.getOffsetsList(i).extractValues();
			int tmpListSize = ubm.getNumOffsets(i);
			for(int k = 0; k < tmpListSize; k++)
				_data[tmpList[k]] = (byte) i;
		}
	}

	// Internal Constructor, to be used when copying a DDC Colgroup, and for scalar operations
	public ColGroupDDC1(int[] colIndices, int numRows, double[] values, byte[] data) {
		super(colIndices, numRows, values);
		_data = data;
	}

	@Override
	public CompressionType getCompType() {
		return CompressionType.DDC1;
	}

	/**
	 * Getter method to get the data, contained in The DDC ColGroup. Not safe if modifications is made to the byte list.
	 * 
	 * @return The contained data
	 */
	public byte[] getData() {
		return _data;
	}

	@Override
	protected double getData(int r) {
		return _values[(_data[r] & 0xFF)];
	}

	@Override
	protected double getData(int r, int colIx) {
		return _values[(_data[r] & 0xFF) * getNumCols() + colIx];
	}

	@Override
	protected void setData(int r, int code) {
		_data[r] = (byte) code;
	}

	@Override
	protected int getCode(int r) {
		return(_data[r] & 0xFF);
	}

	public void recodeData(HashMap<Double, Integer> map) {
		// prepare translation table
		final int numVals = getNumValues();
		byte[] lookup = new byte[numVals];
		for(int k = 0; k < numVals; k++)
			lookup[k] = map.get(_values[k]).byteValue();

		// recode the data
		for(int i = 0; i < _numRows; i++)
			_data[i] = lookup[_data[i] & 0xFF];
	}

	@Override
	public void write(DataOutput out) throws IOException {
		write(out, false);
	}

	@Override
	public void write(DataOutput out, boolean skipDict) throws IOException {
		int numCols = getNumCols();
		int numVals = getNumValues();
		out.writeInt(_numRows);
		out.writeInt(numCols);
		out.writeInt(numVals);

		// write col indices
		for(int i = 0; i < _colIndexes.length; i++)
			out.writeInt(_colIndexes[i]);

		// write distinct values
		if(!skipDict) {
			for(int i = 0; i < _values.length; i++)
				out.writeDouble(_values[i]);
		}

		// write data
		for(int i = 0; i < _numRows; i++)
			out.writeByte(_data[i]);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		readFields(in, false);
	}

	@Override
	public void readFields(DataInput in, boolean skipDict) throws IOException {
		_numRows = in.readInt();
		int numCols = in.readInt();
		int numVals = in.readInt();

		// read col indices
		_colIndexes = new int[numCols];
		for(int i = 0; i < numCols; i++)
			_colIndexes[i] = in.readInt();

		// read distinct values
		if(!skipDict || numCols != 1) {
			_values = new double[numVals * numCols];
			for(int i = 0; i < numVals * numCols; i++)
				_values[i] = in.readDouble();
		}

		// read data
		_data = new byte[_numRows];
		for(int i = 0; i < _numRows; i++)
			_data[i] = in.readByte();
	}

	@Override
	public long getExactSizeOnDisk() {
		long ret = 12; // header
		// col indices
		ret += 4 * _colIndexes.length;
		// distinct values (groups of values)
		ret += 8 * _values.length;
		// data
		ret += 1 * _data.length;

		return ret;
	}

	@Override
	public long estimateInMemorySize() {
		return ColGroupSizes.estimateInMemorySizeDDC1(getNumCols(), getNumRows(), getNumValues(), _data.length);
	}

	@Override
	public void decompressToBlock(MatrixBlock target, int rl, int ru) {
		int ncol = getNumCols();
		for(int i = rl; i < ru; i++)
			for(int j = 0; j < ncol; j++)
				target.appendValue(i, _colIndexes[j], _values[(_data[i] & 0xFF) * ncol + j]);
		// note: append ok because final sort per row
	}

	@Override
	public void decompressToBlock(MatrixBlock target, int colpos) {
		int nrow = getNumRows();
		int ncol = getNumCols();
		double[] c = target.getDenseBlockValues();
		int nnz = 0;
		for(int i = 0; i < nrow; i++)
			nnz += ((c[i] = _values[(_data[i] & 0xFF) * ncol + colpos]) != 0) ? 1 : 0;
		target.setNonZeros(nnz);
	}

	@Override
	public int[] getCounts(int[] counts) {
		return getCounts(0, getNumRows(), counts);
	}

	@Override
	public int[] getCounts(int rl, int ru, int[] counts) {
		final int numVals = getNumValues();
		Arrays.fill(counts, 0, numVals, 0);
		for(int i = rl; i < ru; i++)
			counts[_data[i] & 0xFF]++;
		return counts;
	}

	@Override
	public void countNonZerosPerRow(int[] rnnz, int rl, int ru) {
		final int ncol = getNumCols();
		final int numVals = getNumValues();

		// pre-aggregate nnz per value tuple
		int[] counts = new int[numVals];
		for(int k = 0, valOff = 0; k < numVals; k++, valOff += ncol)
			for(int j = 0; j < ncol; j++)
				counts[k] += (_values[valOff + j] != 0) ? 1 : 0;

		// scan data and add counts to output rows
		for(int i = rl; i < ru; i++)
			rnnz[i - rl] += counts[_data[i] & 0xFF];
	}

	@Override
	public void rightMultByVector(MatrixBlock vector, MatrixBlock result, int rl, int ru) {
		double[] b = ConverterUtils.getDenseVector(vector);
		double[] c = result.getDenseBlockValues();
		final int numCols = getNumCols();
		final int numVals = getNumValues();

		// prepare reduced rhs w/ relevant values
		double[] sb = new double[numCols];
		for(int j = 0; j < numCols; j++) {
			sb[j] = b[_colIndexes[j]];
		}

		// pre-aggregate all distinct values (guaranteed <=255)
		double[] vals = preaggValues(numVals, sb);

		// iterative over codes and add to output
		for(int i = rl; i < ru; i++) {
			c[i] += vals[_data[i] & 0xFF];
		}
	}

	public static void rightMultByVector(ColGroupDDC1[] grps, MatrixBlock vector, MatrixBlock result, int rl, int ru) {
		double[] b = ConverterUtils.getDenseVector(vector);
		double[] c = result.getDenseBlockValues();

		// prepare distinct values once
		double[][] vals = new double[grps.length][];
		for(int i = 0; i < grps.length; i++) {
			// prepare reduced rhs w/ relevant values
			double[] sb = new double[grps[i].getNumCols()];
			for(int j = 0; j < sb.length; j++) {
				sb[j] = b[grps[i]._colIndexes[j]];
			}
			// pre-aggregate all distinct values (guaranteed <=255)
			vals[i] = grps[i].preaggValues(grps[i].getNumValues(), sb, true);
		}

		// cache-conscious matrix-vector multiplication
		// iterative over codes of all groups and add to output
		int blksz = 2048; // 16KB
		for(int bi = rl; bi < ru; bi += blksz)
			for(int j = 0; j < grps.length; j++)
				for(int i = bi; i < Math.min(bi + blksz, ru); i++)
					c[i] += vals[j][grps[j]._data[i] & 0xFF];
	}

	@Override
	public void leftMultByRowVector(MatrixBlock vector, MatrixBlock result) {
		double[] a = ConverterUtils.getDenseVector(vector);
		double[] c = result.getDenseBlockValues();
		final int nrow = getNumRows();
		final int numVals = getNumValues();

		// iterative over codes and pre-aggregate inputs per code (guaranteed <=255)
		// temporary array also avoids false sharing in multi-threaded environments
		double[] vals = allocDVector(numVals, true);
		for(int i = 0; i < nrow; i++) {
			vals[_data[i] & 0xFF] += a[i];
		}

		// post-scaling of pre-aggregate with distinct values
		postScaling(vals, c);
	}

	@Override
	public void leftMultByRowVector(ColGroupDDC a, MatrixBlock result) {
		double[] c = result.getDenseBlockValues();
		final int nrow = getNumRows();
		final int numVals = getNumValues();

		// iterative over codes and pre-aggregate inputs per code (guaranteed <=255)
		// temporary array also avoids false sharing in multi-threaded environments
		double[] vals = allocDVector(numVals, true);
		for(int i = 0; i < nrow; i++)
			vals[_data[i] & 0xFF] += a.getData(i);

		// post-scaling of pre-aggregate with distinct values
		postScaling(vals, c);
	}

	@Override
	protected void computeSum(MatrixBlock result, KahanFunction kplus) {
		final int ncol = getNumCols();
		final int numVals = getNumValues();

		// iterative over codes and count per code (guaranteed <=255)
		int[] counts = getCounts();

		// post-scaling of pre-aggregate with distinct values
		KahanObject kbuff = new KahanObject(result.quickGetValue(0, 0), result.quickGetValue(0, 1));
		for(int k = 0, valOff = 0; k < numVals; k++, valOff += ncol) {
			int cntk = counts[k];
			for(int j = 0; j < ncol; j++)
				kplus.execute3(kbuff, _values[valOff + j], cntk);
		}

		result.quickSetValue(0, 0, kbuff._sum);
		result.quickSetValue(0, 1, kbuff._correction);
	}

	@Override
	protected void computeRowSums(MatrixBlock result, KahanFunction kplus, int rl, int ru) {
		// note: due to corrections the output might be a large dense block
		DenseBlock c = result.getDenseBlock();
		KahanObject kbuff = new KahanObject(0, 0);
		KahanPlus kplus2 = KahanPlus.getKahanPlusFnObject();

		// pre-aggregate nnz per value tuple
		double[] vals = sumAllValues(kplus, kbuff, false);

		// scan data and add to result (use kahan plus not general KahanFunction
		// for correctness in case of sqk+)
		for(int i = rl; i < ru; i++) {
			double[] cvals = c.values(i);
			int cix = c.pos(i);
			kbuff.set(cvals[cix], cvals[cix + 1]);
			kplus2.execute2(kbuff, vals[_data[i] & 0xFF]);
			cvals[cix] = kbuff._sum;
			cvals[cix + 1] = kbuff._correction;
		}
	}

	public static void computeRowSums(ColGroupDDC1[] grps, MatrixBlock result, KahanFunction kplus, int rl, int ru) {
		// note: due to corrections the output might be a large dense block
		DenseBlock c = result.getDenseBlock();
		KahanObject kbuff = new KahanObject(0, 0);
		KahanPlus kplus2 = KahanPlus.getKahanPlusFnObject();

		// prepare distinct values once
		double[][] vals = new double[grps.length][];
		for(int i = 0; i < grps.length; i++) {
			// pre-aggregate all distinct values (guaranteed <=255)
			vals[i] = grps[i].sumAllValues(kplus, kbuff);
		}

		// cache-conscious row sums operations
		// iterative over codes of all groups and add to output
		// (use kahan plus not general KahanFunction for correctness in case of sqk+)
		int blksz = 1024; // 16KB
		double[] tmpAgg = new double[blksz];
		for(int bi = rl; bi < ru; bi += blksz) {
			Arrays.fill(tmpAgg, 0);
			// aggregate all groups
			for(int j = 0; j < grps.length; j++) {
				double[] valsj = vals[j];
				byte[] dataj = grps[j]._data;
				for(int i = bi; i < Math.min(bi + blksz, ru); i++)
					tmpAgg[i - bi] += valsj[dataj[i] & 0xFF];
			}
			// add partial results of all ddc groups
			for(int i = bi; i < Math.min(bi + blksz, ru); i++) {
				double[] cvals = c.values(i);
				int cix = c.pos(i);
				kbuff.set(cvals[cix], cvals[cix + 1]);
				kplus2.execute2(kbuff, tmpAgg[i - bi]);
				cvals[cix] = kbuff._sum;
				cvals[cix + 1] = kbuff._correction;
			}
		}
	}

	@Override
	public ColGroup scalarOperation(ScalarOperator op) {
		// fast path: sparse-safe and -unsafe operations
		// as zero are represented, it is sufficient to simply apply the scalar op
		return new ColGroupDDC1(_colIndexes, _numRows, applyScalarOp(op), _data);
	}
}
