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

import java.util.Arrays;

import org.tugraz.sysds.common.Warnings;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.runtime.util.UtilFunctions;

public class DenseBlockInt64 extends DenseBlockDRB
{
	private static final long serialVersionUID = 7166657802668966730L;

	private long[] _data;

	public DenseBlockInt64(int[] dims) {
		super(dims);
		reset(_rlen, _odims, 0);
	}
	
	public DenseBlockInt64(int[] dims, long[] data) {
		super(dims);
		_data = data;
	}
	
	@Override
	public boolean isNumeric() {
		return true;
	}
	
	@Override
	public void reset(int rlen, int[] odims, double v) {
		long lv = UtilFunctions.toLong(v);
		int len = rlen * odims[0];
		if( len > capacity() ) {
			_data = new long[len];
			if( v != 0 )
				Arrays.fill(_data, lv);
		}
		else {
			Arrays.fill(_data, 0, len, lv);
		}
		_rlen = rlen;
		_odims = odims;
	}

	@Override
	public long capacity() {
		return (_data!=null) ? _data.length : -1;
	}

	@Override
	public long countNonZeros() {
		return UtilFunctions.computeNnz(_data, 0, _rlen*_odims[0]);
	}
	
	@Override
	public int countNonZeros(int r) {
		return UtilFunctions.computeNnz(_data, r*_odims[0], _odims[0]);
	}

	@Override
	public long countNonZeros(int rl, int ru, int ol, int ou) {
		long nnz = 0;
		if( ol == 0 && ou == _odims[0] ) { //specific case: all cols
			nnz += UtilFunctions.computeNnz(_data, rl*_odims[0], (ru-rl)*_odims[0]);
		}
		else {
			for( int i=rl, ix=rl*_odims[0]; i<ru; i++, ix+=_odims[0] )
				nnz += UtilFunctions.computeNnz(_data, ix+ol, ou-ol);
		}
		return nnz;
	}

	@Override
	public double[] values(int r) {
		double[] ret = getReuseRow(false);
		int ix = pos(r);
		int ncol = _odims[0];
		for(int j=0; j<ncol; j++)
			ret[j] = _data[ix+j];
		return ret;
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
	public void incr(int r, int c) {
		_data[pos(r, c)] ++;
	}
	
	@Override
	public void incr(int r, int c, double delta) {
		_data[pos(r, c)] += delta;
	}
	
	@Override
	public DenseBlock set(double v) {
		long lv = UtilFunctions.toLong(v);
		Arrays.fill(_data, 0, _rlen*_odims[0], lv);
		return this;
	}
	
	@Override
	public DenseBlock set(int rl, int ru, int ol, int ou, double v) {
		long lv = UtilFunctions.toLong(v);
		if( ol==0 && ou == _odims[0] )
			Arrays.fill(_data, rl*_odims[0], ru*_odims[0], lv);
		else
			for(int i=rl, ix=rl*_odims[0]; i<ru; i++, ix+=_odims[0])
				Arrays.fill(_data, ix+ol, ix+ou, lv);
		return this;
	}

	@Override
	public DenseBlock set(int r, int c, double v) {
		_data[pos(r, c)] = UtilFunctions.toLong(v);
		return this;
	}
	
	@Override
	public DenseBlock set(DenseBlock db) {
		System.arraycopy(DataConverter.toLong(db.valuesAt(0)), 0, _data, 0, _rlen*_odims[0]);
		return this;
	}
	
	@Override
	public DenseBlock set(int rl, int ru, int ol, int ou, DenseBlock db) {
		double[] a = db.valuesAt(0);
		if( ol == 0 && ou == _odims[0])
			System.arraycopy(a, 0, _data, rl*_odims[0]+ol, (int)db.size());
		else {
			int len = ou - ol;
			for(int i=rl, ix1=0, ix2=rl*_odims[0]+ol; i<ru; i++, ix1+=len, ix2+=_odims[0])
				System.arraycopy(a, ix1, _data, ix2, len);
		}
		return this;
	}

	@Override
	public DenseBlock set(int r, double[] v) {
		System.arraycopy(DataConverter.toLong(v), 0, _data, pos(r), _odims[0]);
		return this;
	}

	@Override
	public DenseBlock set(int[] ix, double v) {
		_data[pos(ix)] = UtilFunctions.toLong(v);
		return this;
	}

	@Override
	public DenseBlock set(int[] ix, String v) {
		_data[pos(ix)] = Long.parseLong(v);
		return this;
	}

	@Override
	public double get(int r, int c) {
		return _data[pos(r, c)];
	}

	@Override
	public double get(int[] ix) {
		return _data[pos(ix)];
	}

	@Override
	public String getString(int[] ix) {
		return String.valueOf(_data[pos(ix)]);
	}
}
