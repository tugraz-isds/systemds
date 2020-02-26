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

package org.tugraz.sysds.runtime.transform.decode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.wink.json4j.JSONObject;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.runtime.transform.TfUtils.TfMethod;
import org.tugraz.sysds.runtime.transform.meta.TfMetaUtils;
import org.tugraz.sysds.runtime.util.UtilFunctions;


public class DecoderFactory 
{
	public static Decoder createDecoder(String spec, String[] colnames, ValueType[] schema, FrameBlock meta) {
		return createDecoder(spec, colnames, schema, meta, meta.getNumColumns());
	}
	
	@SuppressWarnings("unchecked")
	public static Decoder createDecoder(String spec, String[] colnames, ValueType[] schema, FrameBlock meta, int clen) 
	{
		Decoder decoder = null;
		
		try
		{
			//parse transform specification
			JSONObject jSpec = new JSONObject(spec);
			List<Decoder> ldecoders = new ArrayList<>();
			
			//create decoders 'recode', 'dummy' and 'pass-through'
			List<Integer> rcIDs = Arrays.asList(ArrayUtils.toObject(
					TfMetaUtils.parseJsonIDList(jSpec, colnames, TfMethod.RECODE.toString())));
			List<Integer> dcIDs = Arrays.asList(ArrayUtils.toObject(
					TfMetaUtils.parseJsonIDList(jSpec, colnames, TfMethod.DUMMYCODE.toString()))); 
			rcIDs = new ArrayList<Integer>(CollectionUtils.union(rcIDs, dcIDs));
			int len = dcIDs.isEmpty() ? Math.min(meta.getNumColumns(), clen) : meta.getNumColumns();
			List<Integer> ptIDs = new ArrayList<Integer>(CollectionUtils
				.subtract(UtilFunctions.getSeqList(1, len, 1), rcIDs));
			
			//create default schema if unspecified (with double columns for pass-through)
			if( schema == null ) {
				schema = UtilFunctions.nCopies(len, ValueType.STRING);
				for( Integer col : ptIDs )
					schema[col-1] = ValueType.FP64;
			}
			
			if( !dcIDs.isEmpty() ) {
				ldecoders.add(new DecoderDummycode(schema, 
					ArrayUtils.toPrimitive(dcIDs.toArray(new Integer[0]))));
			}
			if( !rcIDs.isEmpty() ) {
				ldecoders.add(new DecoderRecode(schema, !dcIDs.isEmpty(),
					ArrayUtils.toPrimitive(rcIDs.toArray(new Integer[0]))));
			}
			if( !ptIDs.isEmpty() ) {
				ldecoders.add(new DecoderPassThrough(schema, 
					ArrayUtils.toPrimitive(ptIDs.toArray(new Integer[0])),
					ArrayUtils.toPrimitive(dcIDs.toArray(new Integer[0]))));
			}
			
			//create composite decoder of all created decoders
			//and initialize with given meta data (recode, dummy, bin)
			decoder = new DecoderComposite(schema, ldecoders);
			decoder.setColnames(colnames);
			decoder.initMetaData(meta);
		}
		catch(Exception ex) {
			throw new DMLRuntimeException(ex);
		}
		
		return decoder;
	}
}
