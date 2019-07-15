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
		throw new UnsupportedOperationException();
	}

	@Override
	public void incr(int r, int c, double delta) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void fillBlock(int bix, int fromIndex, int toIndex, double v) {
		Arrays.fill(_blocks[bix], fromIndex, toIndex, String.valueOf(v));
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
