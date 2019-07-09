/*
 * Modifications Copyright 2018 Graz University of Technology
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.tugraz.sysds.runtime.data;

import org.tugraz.sysds.common.Warnings;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.runtime.util.UtilFunctions;

import java.util.Arrays;

public class DenseBlockLString extends DenseBlockLDRB
{
	private static final long serialVersionUID = -6632424825959423264L;

	private String[][] _blocks;

	public DenseBlockLString(int[] dims) {
		super(dims);
		reset(_rlen, _odims, 0);
	}
	
	@Override
	public boolean isNumeric() {
		return false;
	}

	@Override
	public boolean isContiguous() {
		return _blocks.length == 1;
	}

	private boolean isReusable(int rlen, int[] odims) {
		if (_blocks == null) return false;
		// The number of rows possible to store in the blocks except the last one
		int possibleRowsPerBlock = _blocks[0].length / odims[0];
		// The number of blocks which will be completely in use after reset
		int neededCompleteBlocks = rlen / possibleRowsPerBlock;
		// The number of rows which will be in use in the last block
		int neededLastBlockSize = rlen % possibleRowsPerBlock;
		if (neededCompleteBlocks > _blocks.length) {
			return false;
		}
		if (neededCompleteBlocks < _blocks.length - 1) {
			// We have enough complete sized blocks to store everything
			return true;
		}
		// The number of rows which fit in the last block
		int lastBlockSize = _blocks[_blocks.length - 1].length / odims[0];
		if (neededCompleteBlocks == _blocks.length - 1) {
			// Check if the last block has the necessary space for our rows
			return neededLastBlockSize <= lastBlockSize;
		}
		if (neededCompleteBlocks == _blocks.length) {
			// Check if we can store enough rows in the last (most likely smaller) block
			// and we don't need another row.
			return neededLastBlockSize == 0 && possibleRowsPerBlock == lastBlockSize;
		}
		return false;
	}

	@Override
	public void reset(int rlen, int[] odims, double v) {
		if(!isReusable(rlen, odims)) {
			// More memory is needed
			int newBlockSize = Integer.MAX_VALUE / odims[0];
			int restBlockSize = rlen % newBlockSize;
			int newNumBlocks = (rlen / newBlockSize) + (restBlockSize == 0 ? 0 : 1);
			if (restBlockSize == 0) {
				_blocks = new String[newNumBlocks][newBlockSize * odims[0]];
			} else {
				_blocks = new String[newNumBlocks][];
				for (int i = 0; i < newNumBlocks - 1; i++) {
					_blocks[i] = new String[newBlockSize * odims[0]];
				}
				_blocks[newNumBlocks - 1] = new String[restBlockSize * odims[0]];
			}
		}
		if (v != 0) {
			for (int bix = 0; bix < numBlocks(); bix++) {
				Arrays.fill(_blocks[bix], String.valueOf(v));
			}
		} else {
			for (int bix = 0; bix < numBlocks(); bix++) {
				Arrays.fill(_blocks[bix], "");
			}
		}
		_rlen = rlen;
		_odims = odims;
	}

	@Override
	public int numBlocks() {
		return _blocks.length;
	}

	@Override
	public int blockSize() {
	    return _blocks[0].length / _odims[0];
	}

	@Override
	public int blockSize(int bix) {
		return _blocks[bix].length / _odims[0];
	}

	@Override
	public long capacity() {
		return (_blocks!=null) ? (long)(_blocks.length - 1) * _blocks[0].length + _blocks[_blocks.length - 1].length : -1;
	}

	@Override
	public long countNonZeros() {
		long nnz = 0;
		for (int i = 0; i < _blocks.length - 1; i++) {
			nnz += UtilFunctions.computeNnz(_blocks[i], 0, blockSize() * _odims[0]);
		}
		return nnz + UtilFunctions.computeNnz(_blocks[_blocks.length - 1], 0,
				blockSize(_blocks.length - 1) * _odims[0]);
	}
	
	@Override
	public int countNonZeros(int r) {
		return UtilFunctions.computeNnz(_blocks[index(r)], pos(r), _odims[0]);
	}

	@Override
	public long countNonZeros(int rl, int ru, int cl, int cu) {
		long nnz = 0;
		boolean allColumns = cl == 0 && cu == _odims[0];
		int rb = pos(rl);
		int re = blockSize() * _odims[0];
		// loop over rows of blocks, and call computeNnz for the specified columns
		for (int bi = index(rl); bi <= index(ru - 1); bi++) {
		    // loop complete block if not last one
			if (bi == index(ru - 1)) {
				re = pos(ru - 1) + _odims[0];
			}
			if (allColumns) {
				nnz += UtilFunctions.computeNnz(_blocks[bi], rb, re - rb) ;
			}
			else {
				for (int ri = rb; ri < re; ri += _odims[0]) {
				    nnz += UtilFunctions.computeNnz(_blocks[bi], ri + cl, cu - cl);
				}
			}
			rb = 0;
		}
		return nnz;
	}

	@Override
	public double[] values(int r) {
		return valuesAt(index(r));
	}
	
	@Override
	public double[] valuesAt(int bix) {
		Warnings.warnFullFP64Conversion(_blocks[bix].length);
		return DataConverter.toDouble(_blocks[bix]);
	}

	@Override
	public void incr(int r, int c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void incr(int r, int c, double delta) {
		throw new UnsupportedOperationException();
	}

	@Override
	public DenseBlock set(double v) {
		for (int i = 0; i < _blocks.length; i++) {
			Arrays.fill(_blocks[i], 0, blockSize(i) * _odims[0], String.valueOf(v));
		}
		return this;
	}

	@Override
	public DenseBlock set(int rl, int ru, int cl, int cu, double v) {
		String vs = String.valueOf(v);
		int rb = pos(rl);
		int re = blockSize() * _odims[0];
		for (int bi = index(rl); bi <= index(ru - 1); bi++) {
			if (bi == index(ru - 1)) {
				re = pos(ru - 1) + _odims[0];
			}
			for (int ri = rb; ri < re; ri += _odims[0]) {
				for (int ci = cl; ci < cu; ci++) {
					_blocks[bi][pos(ri, ci)] = vs;
				}
			}
			rb = 0;
		}
		return this;
	}

	@Override
	public DenseBlock set(int r, int c, double v) {
		_blocks[index(r)][pos(r, c)] = String.valueOf(v);
		return this;
	}

	@Override
	public DenseBlock set(int[] ix, double v) {
	    _blocks[index(ix[0])][pos(ix)] = String.valueOf(v);
		return this;
	}

	@Override
	public DenseBlock set(int[] ix, String v) {
		_blocks[index(ix[0])][pos(ix)] = v;
		return this;
	}

	@Override
	public DenseBlock set(int r, double[] v) {
		System.arraycopy(DataConverter.toString(v), 0, _blocks[index(r)], pos(r), _odims[0]);
		return this;
	}

	@Override
	public DenseBlock set(DenseBlock db) {
		for (int r = 0; r < _rlen; r++) {
			for (int c = 0; c < _odims[0]; c++) {
				_blocks[index(r)][pos(r, c)] = db.getString(new int[]{r, c});
			}
		}
		return this;
	}

	@Override
	public DenseBlock set(int rl, int ru, int cl, int cu, DenseBlock db) {
		int rb = pos(rl);
		int re = blockSize() * _odims[0];
		for (int bi = index(rl); bi <= index(ru - 1); bi++) {
			if (bi == index(ru - 1)) {
				re = pos(ru - 1) + _odims[0];
			}
			else {
				for (int ri = rb; ri < re; ri += _odims[0]) {
					for (int ci = cl; ci < cu; ci++) {
						_blocks[bi][pos(ri, ci)] = db.getString(new int[]{ri, ci});
					}
				}
			}
			rb = 0;
		}
		return this;
	}

	@Override
	public double get(int r, int c) {
		return Double.parseDouble(_blocks[index(r)][pos(r, c)]);
	}

	@Override
	public double get(int[] ix) {
		return Double.parseDouble(_blocks[index(ix[0])][pos(ix)]);
	}

	@Override
	public String getString(int[] ix) {
		return _blocks[index(ix[0])][pos(ix)];
	}
}
