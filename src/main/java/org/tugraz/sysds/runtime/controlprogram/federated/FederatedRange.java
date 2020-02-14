/*
 * Copyright 2019 Graz University of Technology
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
 *
 */

package org.tugraz.sysds.runtime.controlprogram.federated;

import java.util.Arrays;

public class FederatedRange implements Comparable<FederatedRange> {
	private long[] _beginDims;
	private long[] _endDims;
	
	/**
	 * Create a range with the indexes of each dimension between their respective <code>beginDims</code> and
	 * <code>endDims</code> values.
	 * @param beginDims the beginning indexes for each dimension
	 * @param endDims the ending indexes for each dimension
	 */
	public FederatedRange(long[] beginDims, long[] endDims) {
		_beginDims = beginDims;
		_endDims = endDims;
	}
	
	public long[] getBeginDims() {
		return _beginDims;
	}
	
	public long[] getEndDims() {
		return _endDims;
	}
	
	public int[] getBeginDimsInt() {
		return Arrays.stream(_beginDims).mapToInt(i -> (int) i).toArray();
	}
	
	public int[] getEndDimsInt() {
		return Arrays.stream(_endDims).mapToInt(i -> (int) i).toArray();
	}
	
	public long getSize() {
		long size = 1;
		for (int i = 0; i < _beginDims.length; i++) {
			size *= _endDims[i] - _beginDims[i];
		}
		return size;
	}
	
	@Override
	public int compareTo(FederatedRange o) {
		for (int i = 0; i < _beginDims.length; i++) {
			if ( _beginDims[i] < o._beginDims[i])
				return -1;
			if ( _beginDims[i] > o._beginDims[i])
				return 1;
		}
		return 0;
	}
	
	@Override
	public String toString() {
		return Arrays.toString(_beginDims) + " - " + Arrays.toString(_endDims);
	}
}
