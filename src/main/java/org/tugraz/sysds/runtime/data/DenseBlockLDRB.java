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

}
