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

public class DenseBlockString extends DenseBlockDRB {
	private static final long serialVersionUID = 7071870563356352352L;

	private String[] _data;

	public DenseBlockString(int[] dims) {
		super(dims);
		reset(_rlen, _odims, 0);
	}

	public DenseBlockString(int[] dims, String[] data) {
		super(dims);
		_data = data;
	}

	@Override
	public boolean isNumeric() {
		return false;
	}

	@Override
	public void reset(int rlen, int[] odims, double v) {
		int len = rlen * odims[0];
		if (len > capacity()) {
			_data = new String[len];
			// v == 0 will be interpreted as "make empty string"
			if (v != 0) {
				Arrays.fill(_data, String.valueOf(v));
			} else {
				Arrays.fill(_data, "");
			}
		} else {
			if (v != 0) {
				Arrays.fill(_data, String.valueOf(v));
			} else {
				Arrays.fill(_data, "");
			}
		}
		_rlen = rlen;
		_odims = odims;
	}

	@Override
	public long capacity() {
		return (_data != null) ? _data.length : -1;
	}

	@Override
	protected long computeNnz(int bix, int start, int length) {
		return UtilFunctions.computeNnz(_data, start, length);
	}

	@Override
	public double[] values(int r) {
		return DataConverter.toDouble(_data);
	}

	@Override
	public double[] valuesAt(int bix) {
		Warnings.warnFullFP64Conversion(_data.length);
		return DataConverter.toDouble(_data);
	}

	@Override
	public int index(int r) {
		return 0;
	}

	@Override
	public int pos(int r) {
		return r * _odims[0];
	}

	@Override
	public int pos(int r, int c) {
		return r * _odims[0] + c;
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
		Arrays.fill(_data, fromIndex, toIndex, String.valueOf(v));
	}

	@Override
	public DenseBlock set(int r, int c, double v) {
		_data[pos(r, c)] = String.valueOf(v);
		return this;
	}

	@Override
	public DenseBlock set(DenseBlock db) {
		for (int r = 0; r < _rlen; r++) {
			for (int c = 0; c < _odims[0]; c++) {
				_data[pos(r, c)] = db.getString(new int[]{r, c});
			}
		}
		return this;
	}

	@Override
	public DenseBlock set(int rl, int ru, int ol, int ou, DenseBlock db) {
		String[] a = DataConverter.toString(db.valuesAt(0));
		if (ol == 0 && ou == _odims[0])
			System.arraycopy(a, 0, _data, rl * _odims[0] + ol, (int) db.size());
		else {
			int len = ou - ol;
			for (int i = rl, ix1 = 0, ix2 = rl * _odims[0] + ol; i < ru; i++, ix1 += len, ix2 += _odims[0])
				System.arraycopy(a, ix1, _data, ix2, len);
		}
		return this;
	}

	@Override
	public DenseBlock set(int r, double[] v) {
		System.arraycopy(DataConverter.toString(v), 0, _data, pos(r), _odims[0]);
		return this;
	}

	@Override
	public DenseBlock set(int[] ix, double v) {
		_data[pos(ix)] = String.valueOf(v);
		return this;
	}

	@Override
	public double get(int r, int c) {
		return Double.parseDouble(_data[pos(r, c)]);
	}

	@Override
	public double get(int[] ix) {
		return Double.parseDouble(_data[pos(ix)]);
	}

	@Override
	public DenseBlock set(int[] ix, String v) {
		_data[pos(ix)] = v;
		return this;
	}

	@Override
	public String getString(int[] ix) {
		return _data[pos(ix)];
	}
}
