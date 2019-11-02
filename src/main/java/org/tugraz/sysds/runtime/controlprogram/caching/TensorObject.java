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

package org.tugraz.sysds.runtime.controlprogram.caching;


import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.spark.api.java.JavaPairRDD;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.DataType;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.tugraz.sysds.runtime.data.TensorBlock;
import org.tugraz.sysds.runtime.data.TensorIndexes;
import org.tugraz.sysds.runtime.instructions.spark.data.RDDObject;
import org.tugraz.sysds.runtime.io.FileFormatProperties;
import org.tugraz.sysds.runtime.matrix.data.InputInfo;
import org.tugraz.sysds.runtime.matrix.data.OutputInfo;
import org.tugraz.sysds.runtime.meta.DataCharacteristics;
import org.tugraz.sysds.runtime.meta.MetaData;
import org.tugraz.sysds.runtime.meta.MetaDataFormat;
import org.tugraz.sysds.runtime.meta.TensorCharacteristics;
import org.tugraz.sysds.runtime.util.DataConverter;

import java.io.IOException;
import java.util.Arrays;

public class TensorObject extends CacheableData<TensorBlock> {
	private static final long serialVersionUID = -2843358400200380775L;

	protected TensorObject() {
		super(DataType.TENSOR, ValueType.STRING);
	}

	public TensorObject(String fname) {
		this();
		setFileName(fname);
	}

	public TensorObject(ValueType vt, String fname) {
		super(DataType.TENSOR, vt);
		setFileName(fname);
	}
	
	public TensorObject(String fname, MetaData meta) {
		this();
		setFileName(fname);
		setMetaData(meta);
	}

	/**
	 * Copy constructor that copies meta data but NO data.
	 *
	 * @param fo frame object
	 */
	public TensorObject(TensorObject fo) {
		super(fo);
	}

	@Override
	public void refreshMetaData() {
		if ( _data == null || _metaData == null ) //refresh only for existing data
			throw new DMLRuntimeException("Cannot refresh meta data because there is no data or meta data. ");

		//update matrix characteristics
		DataCharacteristics tc = _metaData.getDataCharacteristics();
		long[] dims = _metaData.getDataCharacteristics().getDims();
		tc.setDims(dims);
		tc.setNonZeros(_data.getNonZeros());
	}

	public long getNumRows() {
		DataCharacteristics dc = getDataCharacteristics();
		return dc.getDim(0);
	}

	public long getNumColumns() {
		DataCharacteristics dc = getDataCharacteristics();
		return dc.getDim(1);
	}

	public long getNnz() {
		return getDataCharacteristics().getNonZeros();
	}

	@Override
	protected TensorBlock readBlobFromCache(String fname) throws IOException {
		return (TensorBlock) LazyWriteBuffer.readBlock(fname, false);
	}

	@Override
	protected TensorBlock readBlobFromHDFS(String fname, long[] dims)
			throws IOException {
		MetaDataFormat iimd = (MetaDataFormat) _metaData;
		DataCharacteristics dc = iimd.getDataCharacteristics();
		long begin = 0;

		if( LOG.isTraceEnabled() ) {
			LOG.trace("Reading tensor from HDFS...  " + hashCode() + "  Path: " + fname +
					", dimensions: " + Arrays.toString(dims));
			begin = System.currentTimeMillis();
		}

		int blen = dc.getBlocksize();
		//read tensor and maintain meta data
		TensorBlock newData = DataConverter.readTensorFromHDFS(fname, iimd.getInputInfo(), dims, blen, getSchema());
		setHDFSFileExists(true);

		//sanity check correct output
		if( newData == null )
			throw new IOException("Unable to load tensor from file: "+fname);

		if( LOG.isTraceEnabled() )
			LOG.trace("Reading Completed: " + (System.currentTimeMillis()-begin) + " msec.");

		return newData;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected TensorBlock readBlobFromRDD(RDDObject rdd, MutableBoolean status) {
		status.setValue(false);
		TensorCharacteristics tc = (TensorCharacteristics) _metaData.getDataCharacteristics();
		// TODO correct blocksize;
		// TODO read from RDD
		return SparkExecutionContext.toTensorBlock((JavaPairRDD<TensorIndexes, TensorBlock>) rdd.getRDD(), tc);
	}

	@Override
	protected void writeBlobToHDFS(String fname, String ofmt, int rep, FileFormatProperties fprop)
			throws IOException, DMLRuntimeException {
		long begin = 0;
		if (LOG.isTraceEnabled()) {
			LOG.trace(" Writing tensor to HDFS...  " + hashCode() + "  Path: " + fname + ", Format: " +
					(ofmt != null ? ofmt : "inferred from metadata"));
			begin = System.currentTimeMillis();
		}

		MetaDataFormat iimd = (MetaDataFormat) _metaData;

		if (_data != null) {
			// Get the dimension information from the metadata stored within TensorObject
			DataCharacteristics dc = iimd.getDataCharacteristics();
			// Write the tensor to HDFS in requested format
			OutputInfo oinfo = (ofmt != null ? OutputInfo.stringToOutputInfo(ofmt) :
					InputInfo.getMatchingOutputInfo(iimd.getInputInfo()));

			//TODO check correct blocking
			DataConverter.writeTensorToHDFS(_data, fname, oinfo, dc);
			if( LOG.isTraceEnabled() )
				LOG.trace("Writing tensor to HDFS ("+fname+") - COMPLETED... " + (System.currentTimeMillis()-begin) + " msec.");
		}
		else if (LOG.isTraceEnabled()) {
			LOG.trace("Writing tensor to HDFS (" + fname + ") - NOTHING TO WRITE (_data == null).");
		}
		if( DMLScript.STATISTICS )
			CacheStatistics.incrementHDFSWrites();
	}

	@Override
	protected ValueType[] getSchema() {
		return _data.getSchema();
	}

	@Override
	protected void writeBlobFromRDDtoHDFS(RDDObject rdd, String fname, String ofmt)
			throws DMLRuntimeException {
		//TODO rdd write
	}
}
