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

import java.util.Arrays;

public class DenseBlockLInt32 extends DenseBlockLDRB
{
	private static final long serialVersionUID = 4273439182762731283L;

	private int[][] _blocks;

	public DenseBlockLInt32(int[] dims) {
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
		int iv = UtilFunctions.toInt(v);
		if(!isReusable(rlen, odims)) {
			// ToDo: Reuse code in base class
			// More memory is needed
			int newBlockSize = Integer.MAX_VALUE / odims[0];
			int restBlockSize = rlen % newBlockSize;
			int newNumBlocks = (rlen / newBlockSize) + (restBlockSize == 0 ? 0 : 1);
			if (restBlockSize == 0) {
				_blocks = new int[newNumBlocks][newBlockSize * odims[0]];
			} else {
				_blocks = new int[newNumBlocks][];
				for (int i = 0; i < newNumBlocks - 1; i++) {
					_blocks[i] = new int[newBlockSize * odims[0]];
				}
				_blocks[newNumBlocks - 1] = new int[restBlockSize * odims[0]];
			}
			if( v != 0 ) {
				for (int i = 0; i < newNumBlocks; i++) {
					Arrays.fill(_blocks[i], iv);
				}
			}
		}
		else {
			// Memory is enough, overwrite
			set(v);
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
	public int capacity(int bix) {
		return _blocks.length;
	}

	@Override
	protected long computeNnz(int bix, int start, int length) {
		return UtilFunctions.computeNnz(_blocks[bix], start, length);
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
		incr(r, c, 1);
	}

	@Override
	public void incr(int r, int c, double delta) {
		_blocks[index(r)][pos(r, c)] += delta;
	}

	@Override
	protected void fillBlock(int bix, int fromIndex, int toIndex, double v) {
		Arrays.fill(_blocks[bix], fromIndex, toIndex, UtilFunctions.toInt(v));
	}

	@Override
	public DenseBlock set(int r, int c, double v) {
		_blocks[index(r)][pos(r, c)] = UtilFunctions.toInt(v);
		return this;
	}

	@Override
	public DenseBlock set(int[] ix, double v) {
		_blocks[index(ix[0])][pos(ix)] = UtilFunctions.toInt(v);
		return this;
	}

	@Override
	public DenseBlock set(int[] ix, String v) {
		_blocks[index(ix[0])][pos(ix)] = Integer.parseInt(v);
		return this;
	}

	@Override
	public DenseBlock set(int r, double[] v) {
		System.arraycopy(DataConverter.toInt(v), 0, _blocks[index(r)], pos(r), _odims[0]);
		return this;
	}

	@Override
	public DenseBlock set(DenseBlock db) {
		int[] reuse = DataConverter.toInt(db.valuesAt(0));
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
			System.arraycopy(reuse, srcPos, _blocks[destBi], destPos, rowsToAdd * _odims[0]);
			srcPos += length;
			destPos += length;
			finishedRows += rowsToAdd;
			if (finishedRows >= _rlen) {
				break;
			}
			if (srcPos == db.blockSize(srcBi) * _odims[0]) {
				srcPos = 0;
				srcBi++;
				reuse = DataConverter.toInt(db.valuesAt(srcBi));
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
		boolean allColumns = cl == 0 && cu == _odims[0];
		int rb = pos(rl);
		int re = blockSize() * _odims[0];
		for (int bi = index(rl); bi <= index(ru - 1); bi++) {
			if (bi == index(ru - 1)) {
				re = pos(ru - 1) + _odims[0];
			}
			if (allColumns) {
				System.arraycopy(DataConverter.toInt(db.valuesAt(bi)), rb, _blocks[bi], rb, re - rb);
			}
			else {
				for (int ri = rb; ri < re; ri += _odims[0]) {
					System.arraycopy(DataConverter.toInt(db.valuesAt(bi)), ri + cl, _blocks[bi], ri + cl, cu - cl);
				}
			}
			rb = 0;
		}
		return this;
	}

	@Override
	public double get(int r, int c) {
		return _blocks[index(r)][pos(r, c)];
	}

	@Override
	public double get(int[] ix) {
		return _blocks[index(ix[0])][pos(ix)];
	}

	@Override
	public String getString(int[] ix) {
		return String.valueOf(_blocks[ix[0]][pos(ix)]);
	}
}
