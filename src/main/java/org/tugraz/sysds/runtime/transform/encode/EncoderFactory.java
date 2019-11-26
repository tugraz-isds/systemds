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

package org.tugraz.sysds.runtime.transform.encode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.wink.json4j.JSONObject;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.runtime.transform.TfUtils;
import org.tugraz.sysds.runtime.transform.meta.TfMetaUtils;
import org.tugraz.sysds.runtime.util.UtilFunctions;

public class EncoderFactory 
{
	public static Encoder createEncoder(String spec, String[] colnames, int clen, FrameBlock meta) {
		return createEncoder(spec, colnames, UtilFunctions.nCopies(clen, ValueType.STRING), meta);
	}

	public static Encoder createEncoder(String spec, String[] colnames, ValueType[] schema, int clen, FrameBlock meta) {
		ValueType[] lschema = (schema==null) ? UtilFunctions.nCopies(clen, ValueType.STRING) : schema;
		return createEncoder(spec, colnames, lschema, meta);
	}
	
	@SuppressWarnings("unchecked")
	public static Encoder createEncoder(String spec, String[] colnames, ValueType[] schema, FrameBlock meta) {
		Encoder encoder = null;
		int clen = schema.length;
		
		try {
			//parse transform specification
			JSONObject jSpec = new JSONObject(spec);
			List<Encoder> lencoders = new ArrayList<>();
		
			//prepare basic id lists (recode, feature hash, dummycode, pass-through)
			List<Integer> rcIDs = Arrays.asList(ArrayUtils.toObject(
					TfMetaUtils.parseJsonIDList(jSpec, colnames, TfUtils.TXMETHOD_RECODE)));
			List<Integer>haIDs = Arrays.asList(ArrayUtils.toObject(
					TfMetaUtils.parseJsonIDList(jSpec, colnames, TfUtils.TXMETHOD_HASH)));
			List<Integer> dcIDs = Arrays.asList(ArrayUtils.toObject(
					TfMetaUtils.parseJsonIDList(jSpec, colnames, TfUtils.TXMETHOD_DUMMYCODE))); 
			List<Integer> binIDs = TfMetaUtils.parseBinningColIDs(jSpec, colnames);
			//note: any dummycode column requires recode as preparation, unless it follows binning
			rcIDs = new ArrayList<Integer>(
					CollectionUtils.subtract(
						CollectionUtils.union(rcIDs, CollectionUtils.subtract(dcIDs, binIDs)),
						haIDs
					)
				);
			List<Integer> ptIDs = new ArrayList<Integer>(CollectionUtils.subtract(
					CollectionUtils.subtract(UtilFunctions.getSeqList(1, clen, 1), CollectionUtils.union(rcIDs,haIDs)), binIDs));
			List<Integer> oIDs = Arrays.asList(ArrayUtils.toObject(
					TfMetaUtils.parseJsonIDList(jSpec, colnames, TfUtils.TXMETHOD_OMIT)));
			List<Integer> mvIDs = Arrays.asList(ArrayUtils.toObject(
					TfMetaUtils.parseJsonObjectIDList(jSpec, colnames, TfUtils.TXMETHOD_IMPUTE)));
			
			//create individual encoders
			if( !rcIDs.isEmpty() ) {
				EncoderRecode ra = new EncoderRecode(jSpec, colnames, clen);
				ra.setColList(ArrayUtils.toPrimitive(rcIDs.toArray(new Integer[0])));
				lencoders.add(ra);
			}
			if( !haIDs.isEmpty() ) {
				EncoderRecode ha = new EncoderFeatureHash(jSpec, colnames, clen);
				ha.setColList(ArrayUtils.toPrimitive(haIDs.toArray(new Integer[0])));
				lencoders.add(ha);
			}
			if( !ptIDs.isEmpty() )
				lencoders.add(new EncoderPassThrough(
					ArrayUtils.toPrimitive(ptIDs.toArray(new Integer[0])), clen));
			if( !binIDs.isEmpty() )
				lencoders.add(new EncoderBin(jSpec, colnames, schema.length));
			if( !dcIDs.isEmpty() )
				lencoders.add(new EncoderDummycode(jSpec, colnames, schema.length));
			if( !oIDs.isEmpty() )
				lencoders.add(new EncoderOmit(jSpec, colnames, schema.length));
			if( !mvIDs.isEmpty() ) {
				EncoderMVImpute ma = new EncoderMVImpute(jSpec, colnames, schema.length);
				ma.initRecodeIDList(rcIDs);
				lencoders.add(ma);
			}
			
			//create composite decoder of all created encoders
			encoder = new EncoderComposite(lencoders);
			
			//initialize meta data w/ robustness for superset of cols
			if( meta != null ) {
				String[] colnames2 = meta.getColumnNames();
				if( !TfMetaUtils.isIDSpec(jSpec) && colnames!=null && colnames2!=null 
					&& !ArrayUtils.isEquals(colnames, colnames2) ) 
				{
					HashMap<String, Integer> colPos = getColumnPositions(colnames2);
					//create temporary meta frame block w/ shallow column copy
					FrameBlock meta2 = new FrameBlock(meta.getSchema(), colnames2);
					meta2.setNumRows(meta.getNumRows());
					for( int i=0; i<colnames.length; i++ ) {
						if( !colPos.containsKey(colnames[i]) ) {
							throw new DMLRuntimeException("Column name not found in meta data: "
								+colnames[i]+" (meta: "+Arrays.toString(colnames2)+")");
						}
						int pos = colPos.get(colnames[i]);
						meta2.setColumn(i, meta.getColumn(pos));
						meta2.setColumnMetadata(i, meta.getColumnMetadata(pos));
					}
					meta = meta2;
				}
				encoder.initMetaData(meta);
			}
		}
		catch(Exception ex) {
			throw new DMLRuntimeException(ex);
		}
		
		return encoder;
	}
	
	private static HashMap<String, Integer> getColumnPositions(String[] colnames) {
		HashMap<String, Integer> ret = new HashMap<>();
		for(int i=0; i<colnames.length; i++)
			ret.put(colnames[i], i);
		return ret;
	}
}
