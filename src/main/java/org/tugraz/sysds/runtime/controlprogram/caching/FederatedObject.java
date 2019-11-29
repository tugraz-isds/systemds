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

package org.tugraz.sysds.runtime.controlprogram.caching;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.tugraz.sysds.runtime.controlprogram.federated.FederatedResponse;
import org.tugraz.sysds.runtime.data.FederatedData;
import org.tugraz.sysds.runtime.data.FederatedRange;
import org.tugraz.sysds.runtime.instructions.spark.data.RDDObject;
import org.tugraz.sysds.runtime.io.FileFormatProperties;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.meta.DataCharacteristics;
import org.tugraz.sysds.runtime.meta.MatrixCharacteristics;
import org.tugraz.sysds.runtime.meta.MetaData;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FederatedObject extends CacheableData<MatrixBlock> {
	private static final long serialVersionUID = -3850131932040372465L;
	
	private TreeMap<FederatedRange, FederatedData> _mapping = new TreeMap<>();
	
	public FederatedObject() {
		// TODO other value types
		super(Types.DataType.FEDERATED, Types.ValueType.FP64);
		_metaData = new MetaData(new MatrixCharacteristics());
	}
	
	public FederatedObject(long[] dims, List<Pair<FederatedRange, FederatedData>> workers) {
		// TODO other value types
		this();
		reset(dims, workers);
	}
	
	public void reset(long[] dims, List<Pair<FederatedRange, FederatedData>> workers) {
		// TODO other value types
		getDataCharacteristics().setRows(dims[0]).setCols(dims[1]);
		for (Pair<FederatedRange, FederatedData> t : workers) {
			// TODO support all value types
			_mapping.put(t.getLeft(), t.getRight());
		}
	}
	
	public void prepareWorkers() {
		// TODO let tensor worker ead meta data file to find out dimensions
		for (Map.Entry<FederatedRange, FederatedData> entry : _mapping.entrySet()) {
			FederatedRange range = entry.getKey();
			FederatedData value = entry.getValue();
			if( !value.isInitialized() ) {
				long[] beginDims = range.getBeginDims();
				long[] endDims = range.getEndDims();
				long[] dims = getDataCharacteristics().getDims();
				for (int i = 0; i < dims.length; i++) {
					dims[i] = endDims[i] - beginDims[i];
				}
				// TODO figure out filename
				value.initFederatedData();
			}
		}
	}
	
	@Override
	public void refreshMetaData() {
		if (_metaData == null ) //refresh only for existing data
			throw new DMLRuntimeException("Cannot refresh meta data because there is no data or meta data. ");
		
		//update matrix characteristics
		DataCharacteristics tc = _metaData.getDataCharacteristics();
		long[] dims = _metaData.getDataCharacteristics().getDims();
		tc.setDims(dims);
	}
	
	@Override
	public MatrixBlock acquireRead() {
		long[] dims = getDataCharacteristics().getDims();
		MatrixBlock result = new MatrixBlock((int) dims[0], (int) dims[1], false);
		for (Map.Entry<FederatedRange, FederatedData> entry : _mapping.entrySet()) {
			FederatedRange range = entry.getKey();
			// TODO generalize for n dimensions
			int[] beginDimsInt = range.getBeginDimsInt();
			int[] endDimsInt = range.getEndDimsInt();
			
			FederatedData fd = entry.getValue();
			MatrixBlock multRes;
			if( fd.isInitialized() ) {
				FederatedRequest request = new FederatedRequest(FederatedRequest.FedMethod.TRANSFER);
				FederatedResponse response = fd.executeFederatedOperation(request, true);
				if( !response.isSuccessful() ) {
					throw new DMLRuntimeException("Federated tensor multiplication failed: " + response.getErrorMessage());
				}
				multRes = (MatrixBlock) response.getData();
			}
			else {
				throw new DMLRuntimeException("Federated tensor operations only supported on federated tensorObjects");
			}
			result.copy(beginDimsInt[0], endDimsInt[0] - 1, beginDimsInt[1], endDimsInt[1] - 1, multRes, false);
		}
		return result;
	}
	
	@Override
	public void release() {
		// TODO implement
	}
	
	@Override
	protected MatrixBlock readBlobFromCache(String fname) throws IOException {
		throw new DMLRuntimeException("FederatedObject.readBlobFromCache not implemented");
	}
	
	@Override
	protected MatrixBlock readBlobFromHDFS(String fname, long[] dims) throws IOException {
		throw new DMLRuntimeException("FederatedObject.readBlobFromHDFS not implemented");
	}
	
	@Override
	protected MatrixBlock readBlobFromRDD(RDDObject rdd, MutableBoolean status) throws IOException {
		throw new DMLRuntimeException("FederatedObject.readBlobFromRDD not implemented");
	}
	
	@Override
	protected void writeBlobToHDFS(String fname, String ofmt, int rep, FileFormatProperties fprop) throws IOException {
		throw new DMLRuntimeException("FederatedObject.writeBlobToHDFS not implemented");
	}
	
	@Override
	protected void writeBlobFromRDDtoHDFS(RDDObject rdd, String fname, String ofmt) throws IOException {
		throw new DMLRuntimeException("FederatedObject.writeBlobFromRDDtoHDFS not implemented");
	}
}
