/*
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

package org.tugraz.sysds.runtime.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.runtime.util.HDFSTool;
import org.tugraz.sysds.runtime.util.UtilFunctions;

/**
 * Base class for all format-specific frame readers. Every reader is required to implement the basic 
 * read functionality but might provide additional custom functionality. Any non-default parameters
 * (e.g., CSV read properties) should be passed into custom constructors. There is also a factory
 * for creating format-specific readers. 
 * 
 */
public abstract class FrameReader 
{
	public abstract FrameBlock readFrameFromHDFS( String fname, ValueType[] schema, String[] names, long rlen, long clen)
		throws IOException, DMLRuntimeException;

	public FrameBlock readFrameFromHDFS( String fname, ValueType[] schema, long rlen, long clen )
		throws IOException, DMLRuntimeException
	{
		return readFrameFromHDFS(fname, schema, getDefColNames(schema.length), rlen, clen);
	}

	public FrameBlock readFrameFromHDFS( String fname, long rlen, long clen )
		throws IOException, DMLRuntimeException
	{
		return readFrameFromHDFS(fname, getDefSchema(clen), getDefColNames(clen), rlen, clen);
	}

	public abstract FrameBlock readFrameFromInputStream( InputStream is, ValueType[] schema, String[] names, long rlen, long clen)
		throws IOException, DMLRuntimeException;

	public FrameBlock readFrameFromInputStream( InputStream is, ValueType[] schema, long rlen, long clen )
		throws IOException, DMLRuntimeException
	{
		return readFrameFromInputStream(is, schema, getDefColNames(schema.length), rlen, clen);
	}

	public FrameBlock readFrameFromInputStream( InputStream is, long rlen, long clen )
		throws IOException, DMLRuntimeException
	{
		return readFrameFromInputStream(is, getDefSchema(clen), getDefColNames(clen), rlen, clen);
	}

	public ValueType[] getDefSchema( long clen )
		throws DMLRuntimeException
	{
		int lclen = Math.max((int)clen, 1);
		return UtilFunctions.nCopies(lclen, ValueType.STRING);
	}

	public String[] getDefColNames( long clen )
		throws DMLRuntimeException
	{
		return (clen < 0) ? new String[0] : 
			FrameBlock.createColNames((int)clen);
	}
	
	/**
	 * NOTE: mallocDense controls if the output matrix blocks is fully allocated, this can be redundant
	 * if binary block read and single block. 
	 * 
	 * @param schema schema as array of ValueTypes
	 * @param names column names
	 * @param nrow number of rows
	 * @return frame block
	 * @throws IOException if IOException occurs
	 */
	protected static FrameBlock createOutputFrameBlock(ValueType[] schema, String[] names, long nrow)
		throws IOException
	{
		//check schema and column names
		if( !OptimizerUtils.isValidCPDimensions(schema, names) )
			throw new DMLRuntimeException("Schema and names to be define with equal size.");
		
		//prepare result frame block
		FrameBlock ret = new FrameBlock(schema, names);
		ret.ensureAllocatedColumns((int)nrow);
		return ret;
	}

	protected static ValueType[] createOutputSchema(ValueType[] schema, long ncol) {
		if( schema.length==1 && ncol > 1 )
			return UtilFunctions.nCopies((int)ncol, schema[0]);
		return schema;
	}

	protected static String[] createOutputNames(String[] names, long ncol) {
		if( names.length != ncol )
			return FrameBlock.createColNames((int)ncol);
		return names;
	}

	protected static void checkValidInputFile(FileSystem fs, Path path) 
		throws IOException
	{
		//check non-existing file
		if( !fs.exists(path) )	
			throw new IOException("File "+path.toString()+" does not exist on HDFS/LFS.");
	
		//check for empty file
		if( HDFSTool.isFileEmpty(fs, path) )
			throw new EOFException("Empty input file "+ path.toString() +".");		
	}
}
