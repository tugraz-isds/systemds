/*
 * Modifications Copyright 2019 Graz University of Technology
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

import java.util.BitSet;

public class DenseBlockLBool extends DenseBlockLDRB
{
	private static final long serialVersionUID = 2604223782138590322L;

	private BitSet[] _blocks;
	private int _fullBlockSize;
	private int _lastBlockSize;

	public DenseBlockLBool(int[] dims) {
		super(dims);
		reset(_rlen, _odims, 0);
	}

	@Override
	public boolean isNumeric() {
		return true;
	}

	@Override
	public boolean isContiguous() {
		return _blocks.length == 1;
	}

	@Override
	public void reset(int rlen, int[] odims, double v) {
		boolean bv = v != 0;
		if(!isReusable(rlen, odims)) {
			int newBlockSize = Integer.MAX_VALUE / odims[0];
			int restBlockSize = rlen % newBlockSize;
			int newNumBlocks = (rlen / newBlockSize) + (restBlockSize == 0 ? 0 : 1);
			_fullBlockSize = newBlockSize;
			if (restBlockSize == 0) {
				_lastBlockSize = newBlockSize;
				_blocks = new BitSet[newNumBlocks];
				for (int i = 0; i < newNumBlocks; i++) {
					_blocks[i] = new BitSet(newBlockSize * odims[0] + 1);
					_blocks[i].set(newBlockSize * odims[0] + 1);
				}
			} else {
				_lastBlockSize = restBlockSize;
				_blocks = new BitSet[newNumBlocks];
				for (int i = 0; i < newNumBlocks - 1; i++) {
					_blocks[i] = new BitSet(newBlockSize * odims[0] + 1);
					_blocks[i].set(newBlockSize * odims[0] + 1);
				}
				_blocks[newNumBlocks - 1] = new BitSet(restBlockSize * odims[0] + 1);
				_blocks[newNumBlocks - 1].set(restBlockSize * odims[0] + 1);
			}
			if( bv ) {
				for (int i = 0; i < newNumBlocks; i++) {
					_blocks[i].set(0, _blocks[i].length() - 1);
				}
			}
		}
		else {
			// Memory is enough, overwrite
			int newBlockSize = Integer.MAX_VALUE - 1 / odims[0];
			int restBlockSize = rlen % newBlockSize;
			int newNumBlocks = (rlen / newBlockSize) + (restBlockSize == 0 ? 0 : 1);
			_fullBlockSize = newBlockSize;
			if (restBlockSize == 0) {
				_lastBlockSize = newBlockSize;
				for (int i = 0; i < newNumBlocks; i++) {
					_blocks[i].set(0, newBlockSize * odims[0], bv);
					_blocks[i].set(newBlockSize * odims[0] + 1);
				}
			} else {
				_lastBlockSize = restBlockSize;
				for (int i = 0; i < newNumBlocks - 1; i++) {
					_blocks[i].set(0, newBlockSize * odims[0], bv);
					_blocks[i].set(newBlockSize * odims[0] + 1);
				}
				_blocks[newNumBlocks - 1].set(0, restBlockSize * odims[0], bv);
				_blocks[newNumBlocks - 1].set(restBlockSize * odims[0] + 1);
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
		return (_blocks.length == 1) ? _lastBlockSize : _fullBlockSize;
	}

	@Override
	public int blockSize(int bix) {
		return (bix == _blocks.length - 1) ? _lastBlockSize : _fullBlockSize;
	}

	@Override
	public long capacity() {
		return (_blocks!=null) ? (long)(_blocks.length - 1) * _blocks[0].size() + _blocks[_blocks.length - 1].size() : -1;
	}

	@Override
	public int capacity(int bix) {
		return _blocks[bix].size();
	}

	@Override
	protected long computeNnz(int bix, int start, int length) {
		// ToDo: Switch to cardinality?
		return UtilFunctions.computeNnz(_blocks[bix], start, length);
	}

	@Override
	public double[] values(int r) {
		return valuesAt(index(r));
	}
	
	@Override
	public double[] valuesAt(int bix) {
		Warnings.warnFullFP64Conversion(_blocks[bix].length() - 1);
		return DataConverter.toDouble(_blocks[bix], _blocks[bix].length() - 1);
	}

	@Override
	public void incr(int r, int c) {
		_blocks[index(r)].set(pos(r, c));
	}

	@Override
	public void incr(int r, int c, double delta) {
		if (delta != 0) {
			_blocks[index(r)].set(pos(r, c));
		}
	}

	@Override
	protected void fillBlock(int bix, int fromIndex, int toIndex, double v) {
		_blocks[bix].set(fromIndex, toIndex, v != 0);
	}

	@Override
	public DenseBlock set(int r, int c, double v) {
		_blocks[index(r)].set(pos(r, c), v != 0);
		return this;
	}

	@Override
	public DenseBlock set(int[] ix, double v) {
		_blocks[index(ix[0])].set(pos(ix), v != 0);
		return this;
	}

	@Override
	public DenseBlock set(int[] ix, String v) {
		_blocks[index(ix[0])].set(pos(ix), Boolean.parseBoolean(v));
		return this;
	}

	@Override
	public DenseBlock set(int r, double[] v) {
		int ri = r * _odims[0];
		for (int i = ri; i < ri + v.length; i++) {
			_blocks[index(r)].set(i, v[i - ri] != 0);
		}
		return this;
	}

	@Override
	public DenseBlock set(DenseBlock db) {
		double[] reuse = db.valuesAt(0);
		int srcPos = 0;
		int srcBi = 0;
		int destPos = 0;
		int destBi = 0;
		int finishedRows = 0;
		// Loop through both blocks (source and destination) and always take the maximum number of rows that were not
		// yet read from source or not yet written to destination. This procedure is necessary because although the dimensions
		// should match there is the option that one DenseBlock was reset() and reused old blocks, therefore it is possible
		// that one DenseBlock uses a block to represent x rows the other one can actually fit x+y rows in a block.
		while (true) {
			int length;
			if (srcPos != 0) {
				int srcLength = db.blockSize(srcBi) * _odims[0];
				int srcDataLeft = srcLength - srcPos;
				length = Math.min(srcDataLeft, blockSize(destBi) * _odims[0]);
			} else if (destPos != 0) {
				int destLength = blockSize(destBi) * _odims[0];
				int destDataLeft = destLength - destPos;
				length = Math.min(destDataLeft, blockSize(srcBi) * _odims[0]);
			} else {
				length = Math.min(blockSize(srcBi), blockSize(destBi)) * _odims[0];
			}
			int rowsToAdd = Math.min(_rlen - finishedRows, length / _odims[0]);
			for (int i = 0; i < rowsToAdd * _odims[0]; i++) {
				_blocks[destBi].set(destPos + i, reuse[srcPos + i] != 0);
			}
			srcPos += length;
			destPos += length;
			finishedRows += rowsToAdd;
			if (finishedRows >= _rlen) {
				break;
			}
			if (srcPos == db.blockSize(srcBi) * _odims[0]) {
				srcPos = 0;
				srcBi++;
				reuse = db.valuesAt(srcBi);
			}
			if (destPos == blockSize(destBi) * _odims[0]) {
				destPos = 0;
				destBi++;
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
			double[] a = db.valuesAt(bi);
			for (int r = rb; r < re; r += _odims[0]) {
				for (int c = cl; c < cu; c++) {
					int i = r + c;
					_blocks[bi].set(i, a[i] != 0);
				}
			}
			rb = 0;
		}
		return this;
	}

	@Override
	public double get(int r, int c) {
		return _blocks[index(r)].get(pos(r, c)) ? 1 : 0;
	}

	@Override
	public double get(int[] ix) {
		return _blocks[index(ix[0])].get(pos(ix)) ? 1 : 0;
	}

	@Override
	public String getString(int[] ix) {
		return String.valueOf(_blocks[index(ix[0])].get(pos(ix)));
	}
}
