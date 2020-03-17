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

import java.util.List;
import java.util.Arrays;

import org.tugraz.sysds.runtime.compress.BitmapEncoder;

public class DenseRowIterator extends RowIterator<double[]> {

	private final double[] _ret;

	public DenseRowIterator(int rl, int ru, List<ColGroup> colGroups, int clen) {
		super(rl, ru, colGroups);
		_ret = new double[clen];
	}

	@Override
	public double[] next() {
		// prepare meta data common across column groups
		final int blksz = BitmapEncoder.BITMAP_BLOCK_SZ;
		final int ix = _rpos % blksz;
		final boolean last = (_rpos + 1 == _ru);
		// copy group rows into consolidated row
		Arrays.fill(_ret, 0);
		for(int j = 0; j < _iters.length; j++)
			_iters[j].next(_ret, _rpos, ix, last);
		// advance to next row and return buffer
		_rpos++;
		return _ret;
	}
}