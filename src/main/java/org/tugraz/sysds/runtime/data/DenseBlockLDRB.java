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


import org.tugraz.sysds.runtime.util.UtilFunctions;

/**
 * Dense Large Row Blocks have multiple 1D arrays (blocks), which contain complete rows.
 * Except the last block all blocks have the same size (size refers to the number of rows contained and space allocated).
 */
public abstract class DenseBlockLDRB extends DenseBlock
{
	private static final long serialVersionUID = -7519435549328146356L;

	protected DenseBlockLDRB(int[] dims) {
		super(dims);
	}

	/**
	 * Get the length of a allocated block.
	 *
	 * @param bix   block id
	 * @return	  capacity
	 */
	public abstract int capacity(int bix);

	/**
	 * Determine if the old blocks can be reused.
	 *
	 * @param rlen  the new rlen
	 * @param odims the new other dimensions
	 * @return      if the old blocks can be reused
	 */
	protected boolean isReusable(int rlen, int[] odims) {
		if (capacity() == -1) return false;
		// The number of rows possible to store in the blocks except the last one
		int possibleRowsPerBlock = capacity(0) / odims[0];
		// The number of blocks which will be completely in use after reset
		int neededCompleteBlocks = rlen / possibleRowsPerBlock;
		// The number of rows which will be in use in the last block
		int neededLastBlockSize = rlen % possibleRowsPerBlock;
		// The number of rows which fit in the last block
		int lastBlockSize = capacity(numBlocks() - 1) / odims[0];
		boolean reusable = false;
		if (neededCompleteBlocks > numBlocks()) { }
		else if (neededCompleteBlocks < numBlocks() - 1) {
			// We have enough complete sized blocks to store everything
			reusable = true;
		}
		else if (neededCompleteBlocks == numBlocks() - 1) {
			// Check if the last block has the necessary space for our rows
			reusable = neededLastBlockSize <= lastBlockSize;
		}
		else if (neededCompleteBlocks == numBlocks()) {
			// Check if we can store enough rows in the last (most likely smaller) block
			// and we don't need another row.
			reusable = neededLastBlockSize == 0 && possibleRowsPerBlock == lastBlockSize;
		}
		return reusable;
	}

	@Override
	public int pos(int[] ix) {
		int pos = pos(ix[0]);
		pos += ix[ix.length - 1];
		for(int i = 1; i < ix.length - 1; i++)
			pos += ix[i] * _odims[i];
		return pos;
	}

	@Override
	public boolean isContiguous(int rl, int ru) {
		return index(rl) == index(ru);
	}

	@Override
	public int size(int bix) {
		return blockSize(bix) * _odims[0];
	}

	@Override
	public int index(int r) {
		return r / blockSize();
	}

	@Override
	public int pos(int r) {
		return (r % blockSize()) * _odims[0];
	}

	@Override
	public int pos(int r, int c) {
		return (r % blockSize()) * _odims[0] + c;
	}

	@Override
	public long countNonZeros() {
		long nnz = 0;
		for (int i = 0; i < numBlocks() - 1; i++) {
			nnz += computeNnz(i, 0, blockSize() * _odims[0]);
		}
		return nnz + computeNnz(numBlocks() - 1, 0, blockSize(numBlocks() - 1) * _odims[0]);
	}

	public int countNonZeros(int r) {
		return (int) computeNnz(index(r), pos(r), _odims[0]);
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
				nnz += computeNnz(bi, rb, re - rb);
			} else {
				for (int ri = rb; ri < re; ri += _odims[0]) {
					nnz += computeNnz(bi, ri + cl, cu - cl);
				}
			}
			rb = 0;
		}
		return nnz;
	}

	@Override
	public DenseBlock set(double v) {
		for (int i = 0; i < numBlocks() - 1; i++) {
			fillBlock(i, 0, blockSize() * _odims[0], v);
		}
		fillBlock(numBlocks() - 1, 0, blockSize(numBlocks() - 1) * _odims[0], v);
		return this;
	}

	@Override
	public DenseBlock set(int rl, int ru, int cl, int cu, double v) {
		boolean allColumns = cl == 0 && cu == _odims[0];
		int rb = pos(rl);
		int re = blockSize() * _odims[0];
		for (int bi = index(rl); bi <= index(ru - 1); bi++) {
			if (bi == index(ru - 1)) {
				re = pos(ru - 1) + _odims[0];
			}
			if (allColumns) {
				fillBlock(bi, rb, re, v);
			}
			else {
				for (int ri = rb; ri < re; ri += _odims[0]) {
					fillBlock(bi, ri + cl, ri + cu, v);
				}
			}
			rb = 0;
		}
		return this;
	}
}
