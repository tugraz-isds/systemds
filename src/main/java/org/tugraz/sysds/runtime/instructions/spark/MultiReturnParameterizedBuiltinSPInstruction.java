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

package org.tugraz.sysds.runtime.instructions.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.AccumulatorV2;
import org.tugraz.sysds.common.Types.DataType;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.caching.FrameObject;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.tugraz.sysds.runtime.functionobjects.KahanPlus;
import org.tugraz.sysds.runtime.instructions.InstructionUtils;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;
import org.tugraz.sysds.runtime.instructions.cp.KahanObject;
import org.tugraz.sysds.runtime.instructions.spark.ParameterizedBuiltinSPInstruction.RDDTransformApplyFunction;
import org.tugraz.sysds.runtime.instructions.spark.ParameterizedBuiltinSPInstruction.RDDTransformApplyOffsetFunction;
import org.tugraz.sysds.runtime.instructions.spark.utils.FrameRDDConverterUtils;
import org.tugraz.sysds.runtime.instructions.spark.utils.SparkUtils;
import org.tugraz.sysds.runtime.io.FrameReader;
import org.tugraz.sysds.runtime.io.FrameReaderFactory;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock.ColumnMetadata;
import org.tugraz.sysds.runtime.matrix.data.InputInfo;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.data.MatrixIndexes;
import org.tugraz.sysds.runtime.matrix.operators.Operator;
import org.tugraz.sysds.runtime.meta.DataCharacteristics;
import org.tugraz.sysds.runtime.transform.encode.Encoder;
import org.tugraz.sysds.runtime.transform.encode.EncoderComposite;
import org.tugraz.sysds.runtime.transform.encode.EncoderFactory;
import org.tugraz.sysds.runtime.transform.encode.EncoderMVImpute;
import org.tugraz.sysds.runtime.transform.encode.EncoderMVImpute.MVMethod;
import org.tugraz.sysds.runtime.transform.encode.EncoderRecode;
import org.tugraz.sysds.runtime.transform.meta.TfMetaUtils;
import org.tugraz.sysds.runtime.transform.meta.TfOffsetMap;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

public class MultiReturnParameterizedBuiltinSPInstruction extends ComputationSPInstruction {
	protected ArrayList<CPOperand> _outputs;

	private MultiReturnParameterizedBuiltinSPInstruction(Operator op, CPOperand input1, CPOperand input2,
			ArrayList<CPOperand> outputs, String opcode, String istr) {
		super(SPType.MultiReturnBuiltin, op, input1, input2, outputs.get(0), opcode, istr);
		_outputs = outputs;
	}

	public static MultiReturnParameterizedBuiltinSPInstruction parseInstruction( String str ) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		ArrayList<CPOperand> outputs = new ArrayList<>();
		String opcode = parts[0];
		
		if ( opcode.equalsIgnoreCase("transformencode") ) {
			// one input and two outputs
			CPOperand in1 = new CPOperand(parts[1]);
			CPOperand in2 = new CPOperand(parts[2]);
			outputs.add ( new CPOperand(parts[3], ValueType.FP64, DataType.MATRIX) );
			outputs.add ( new CPOperand(parts[4], ValueType.STRING, DataType.FRAME) );
			return new MultiReturnParameterizedBuiltinSPInstruction(null, in1, in2, outputs, opcode, str);
		}
		else {
			throw new DMLRuntimeException("Invalid opcode in MultiReturnBuiltin instruction: " + opcode);
		}

	}

	@Override 
	@SuppressWarnings("unchecked")
	public void processInstruction(ExecutionContext ec) {
		SparkExecutionContext sec = (SparkExecutionContext) ec;
		
		try
		{
			//get input RDD and meta data
			FrameObject fo = sec.getFrameObject(input1.getName());
			FrameObject fometa = sec.getFrameObject(_outputs.get(1).getName());
			JavaPairRDD<Long,FrameBlock> in = (JavaPairRDD<Long,FrameBlock>)
				sec.getRDDHandleForFrameObject(fo, InputInfo.BinaryBlockInputInfo);
			String spec = ec.getScalarInput(input2).getStringValue();
			DataCharacteristics mcIn = sec.getDataCharacteristics(input1.getName());
			DataCharacteristics mcOut = sec.getDataCharacteristics(output.getName());
			String[] colnames = !TfMetaUtils.isIDSpec(spec) ?
				in.lookup(1L).get(0).getColumnNames() : null; 
			
			//step 1: build transform meta data
			Encoder encoderBuild = EncoderFactory.createEncoder(spec, colnames,
				fo.getSchema(), (int)fo.getNumColumns(), null);
			
			MaxLongAccumulator accMax = registerMaxLongAccumulator(sec.getSparkContext()); 
			JavaRDD<String> rcMaps = in
				.mapPartitionsToPair(new TransformEncodeBuildFunction(encoderBuild))
				.distinct().groupByKey()
				.flatMap(new TransformEncodeGroupFunction(accMax));
			if( containsMVImputeEncoder(encoderBuild) ) {
				EncoderMVImpute mva = getMVImputeEncoder(encoderBuild);
				rcMaps = rcMaps.union(
					in.mapPartitionsToPair(new TransformEncodeBuild2Function(mva))
					  .groupByKey().flatMap(new TransformEncodeGroup2Function(mva)) );
			}
			rcMaps.saveAsTextFile(fometa.getFileName()); //trigger eval
			
			//consolidate meta data frame (reuse multi-threaded reader, special handling missing values) 
			FrameReader reader = FrameReaderFactory.createFrameReader(InputInfo.TextCellInputInfo);
			FrameBlock meta = reader.readFrameFromHDFS(fometa.getFileName(), accMax.value(), fo.getNumColumns());
			meta.recomputeColumnCardinality(); //recompute num distinct items per column
			meta.setColumnNames((colnames!=null)?colnames:meta.getColumnNames());
			
			//step 2: transform apply (similar to spark transformapply)
			//compute omit offset map for block shifts
			TfOffsetMap omap = null;
			if( TfMetaUtils.containsOmitSpec(spec, colnames) ) {
				omap = new TfOffsetMap(SparkUtils.toIndexedLong(in.mapToPair(
					new RDDTransformApplyOffsetFunction(spec, colnames)).collect()));
			}
			
			//create encoder broadcast (avoiding replication per task) 
			Encoder encoder = EncoderFactory.createEncoder(spec, colnames,
				fo.getSchema(), (int)fo.getNumColumns(), meta);
			mcOut.setDimension(mcIn.getRows()-((omap!=null)?omap.getNumRmRows():0), encoder.getNumCols()); 
			Broadcast<Encoder> bmeta = sec.getSparkContext().broadcast(encoder);
			Broadcast<TfOffsetMap> bomap = (omap!=null) ? sec.getSparkContext().broadcast(omap) : null;
			
			//execute transform apply
			JavaPairRDD<Long,FrameBlock> tmp = in
				.mapToPair(new RDDTransformApplyFunction(bmeta, bomap));
			JavaPairRDD<MatrixIndexes,MatrixBlock> out = FrameRDDConverterUtils
				.binaryBlockToMatrixBlock(tmp, mcOut, mcOut);
			
			//set output and maintain lineage/output characteristics
			sec.setRDDHandleForVariable(_outputs.get(0).getName(), out);
			sec.addLineageRDD(_outputs.get(0).getName(), input1.getName());
			sec.setFrameOutput(_outputs.get(1).getName(), meta);
		}
		catch(IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	private static boolean containsMVImputeEncoder(Encoder encoder) {
		if( encoder instanceof EncoderComposite )
			for( Encoder cencoder : ((EncoderComposite)encoder).getEncoders() )
				if( cencoder instanceof EncoderMVImpute )
					return true;
		return false;
	}

	private static EncoderMVImpute getMVImputeEncoder(Encoder encoder) {
		if( encoder instanceof EncoderComposite )
			for( Encoder cencoder : ((EncoderComposite)encoder).getEncoders() )
				if( cencoder instanceof EncoderMVImpute )
					return (EncoderMVImpute) cencoder;
		return null;
	}
	
	private static MaxLongAccumulator registerMaxLongAccumulator(JavaSparkContext sc) {
		MaxLongAccumulator acc = new MaxLongAccumulator(Long.MIN_VALUE);
		sc.sc().register(acc, "max");
		return acc;
	}
	

	private static class MaxLongAccumulator extends AccumulatorV2<Long,Long>
	{
		private static final long serialVersionUID = -3739727823287550826L;

		private long _value = Long.MIN_VALUE;
		
		public MaxLongAccumulator(long value) {
			_value = value;
		}

		@Override
		public void add(Long arg0) {
			_value = Math.max(_value, arg0);
		}

		@Override
		public AccumulatorV2<Long, Long> copy() {
			return new MaxLongAccumulator(_value);
		}

		@Override
		public boolean isZero() {
			return _value == Long.MIN_VALUE;
		}

		@Override
		public void merge(AccumulatorV2<Long, Long> arg0) {
			_value = Math.max(_value, arg0.value());
		}

		@Override
		public void reset() {
			_value = Long.MIN_VALUE;
		}

		@Override
		public Long value() {
			return _value;
		}
	}
	
	/**
	 * This function pre-aggregates distinct values of recoded columns per partition
	 * (part of distributed recode map construction, used for recoding, binning and 
	 * dummy coding). We operate directly over schema-specific objects to avoid 
	 * unnecessary string conversion, as well as reduce memory overhead and shuffle.
	 */
	public static class TransformEncodeBuildFunction 
		implements PairFlatMapFunction<Iterator<Tuple2<Long, FrameBlock>>, Integer, Object>
	{
		private static final long serialVersionUID = 6336375833412029279L;

		private EncoderRecode _raEncoder = null;
		
		public TransformEncodeBuildFunction(Encoder encoder) {
			for( Encoder cEncoder : ((EncoderComposite)encoder).getEncoders() )
				if( cEncoder instanceof EncoderRecode )
					_raEncoder = (EncoderRecode)cEncoder;
		}
		
		@Override
		public Iterator<Tuple2<Integer, Object>> call(Iterator<Tuple2<Long, FrameBlock>> iter)
			throws Exception 
		{
			//build meta data (e.g., recode maps)
			if( _raEncoder != null )
				while( iter.hasNext() )
					_raEncoder.buildPartial(iter.next()._2());
			
			//output recode maps as columnID - token pairs
			ArrayList<Tuple2<Integer,Object>> ret = new ArrayList<>();
			HashMap<Integer,HashSet<Object>> tmp = _raEncoder.getCPRecodeMapsPartial();
			for( Entry<Integer,HashSet<Object>> e1 : tmp.entrySet() )
				for( Object token : e1.getValue() )
					ret.add(new Tuple2<>(e1.getKey(), token));
			if( _raEncoder != null )
				_raEncoder.getCPRecodeMapsPartial().clear();
		
			return ret.iterator();
		}
	}
	
	/**
	 * This function assigns codes to globally distinct values of recoded columns 
	 * and writes the resulting column map in textcell (IJV) format to the output. 
	 * (part of distributed recode map construction, used for recoding, binning and 
	 * dummy coding). We operate directly over schema-specific objects to avoid 
	 * unnecessary string conversion, as well as reduce memory overhead and shuffle.
	 */
	public static class TransformEncodeGroupFunction 
		implements FlatMapFunction<Tuple2<Integer, Iterable<Object>>, String>
	{
		private static final long serialVersionUID = -1034187226023517119L;

		private MaxLongAccumulator _accMax = null;
		
		public TransformEncodeGroupFunction( MaxLongAccumulator accMax ) {
			_accMax = accMax;
		}
		
		@Override
		public Iterator<String> call(Tuple2<Integer, Iterable<Object>> arg0)
			throws Exception 
		{
			String colID = String.valueOf(arg0._1());
			Iterator<Object> iter = arg0._2().iterator();
			
			ArrayList<String> ret = new ArrayList<>();
			StringBuilder sb = new StringBuilder();
			long rowID = 1;
			while( iter.hasNext() ) {
				sb.append(rowID);
				sb.append(' ');
				sb.append(colID);
				sb.append(' ');
				sb.append(EncoderRecode.constructRecodeMapEntry(
						iter.next().toString(), rowID));
				ret.add(sb.toString());
				sb.setLength(0); 
				rowID++;
			}
			_accMax.add(rowID-1);
			
			return ret.iterator();
		}
	}

	public static class TransformEncodeBuild2Function implements PairFlatMapFunction<Iterator<Tuple2<Long, FrameBlock>>, Integer, ColumnMetadata>
	{
		private static final long serialVersionUID = 6336375833412029279L;

		private EncoderMVImpute _encoder = null;
		
		public TransformEncodeBuild2Function(EncoderMVImpute encoder) {
			_encoder = encoder;
		}
		
		@Override
		public Iterator<Tuple2<Integer, ColumnMetadata>> call(Iterator<Tuple2<Long, FrameBlock>> iter)
			throws Exception 
		{
			//build meta data (e.g., histograms and means)
			while( iter.hasNext() ) {
				FrameBlock block = iter.next()._2();
				_encoder.build(block);	
			}
			
			//extract meta data
			ArrayList<Tuple2<Integer,ColumnMetadata>> ret = new ArrayList<>();
			int[] collist = _encoder.getColList();
			for( int j=0; j<collist.length; j++ ) {
				if( _encoder.getMethod(collist[j]) == MVMethod.GLOBAL_MODE ) {
					HashMap<String,Long> hist = _encoder.getHistogram(collist[j]);
					for( Entry<String,Long> e : hist.entrySet() )
						ret.add(new Tuple2<>(collist[j], 
								new ColumnMetadata(e.getValue(), e.getKey())));
				}
				else if( _encoder.getMethod(collist[j]) == MVMethod.GLOBAL_MEAN ) {
					ret.add(new Tuple2<>(collist[j], 
							new ColumnMetadata(_encoder.getNonMVCount(collist[j]), String.valueOf(_encoder.getMeans()[j]._sum))));
				}
				else if( _encoder.getMethod(collist[j]) == MVMethod.CONSTANT ) {
					ret.add(new Tuple2<>(collist[j],
							new ColumnMetadata(0, _encoder.getReplacement(collist[j]))));
				}
			}
			
			return ret.iterator();
		}
	}

	public static class TransformEncodeGroup2Function implements FlatMapFunction<Tuple2<Integer, Iterable<ColumnMetadata>>, String>
	{
		private static final long serialVersionUID = 702100641492347459L;
		
		private EncoderMVImpute _encoder = null;
		
		public TransformEncodeGroup2Function(EncoderMVImpute encoder) {	
			_encoder = encoder;
		}

		@Override
		public Iterator<String> call(Tuple2<Integer, Iterable<ColumnMetadata>> arg0)
				throws Exception 
		{
			int colix = arg0._1();
			Iterator<ColumnMetadata> iter = arg0._2().iterator();
			ArrayList<String> ret = new ArrayList<>();
			
			//compute global mode of categorical feature, i.e., value with highest frequency
			if( _encoder.getMethod(colix) == MVMethod.GLOBAL_MODE ) {
				HashMap<String, Long> hist = new HashMap<>();
				while( iter.hasNext() ) {
					ColumnMetadata cmeta = iter.next(); 
					Long tmp = hist.get(cmeta.getMvValue());
					hist.put(cmeta.getMvValue(), cmeta.getNumDistinct() + ((tmp!=null)?tmp:0));
				}
				long max = Long.MIN_VALUE; String mode = null;
				for( Entry<String, Long> e : hist.entrySet() ) 
					if( e.getValue() > max  ) {
						mode = e.getKey();
						max = e.getValue();
					}
				ret.add("-2 " + colix + " " + mode);
			}
			//compute global mean of categorical feature
			else if( _encoder.getMethod(colix) == MVMethod.GLOBAL_MEAN ) {
				KahanObject kbuff = new KahanObject(0, 0);
				KahanPlus kplus = KahanPlus.getKahanPlusFnObject();
				int count = 0;
				while( iter.hasNext() ) {
					ColumnMetadata cmeta = iter.next(); 
					kplus.execute2(kbuff, Double.parseDouble(cmeta.getMvValue()));
					count += cmeta.getNumDistinct();
				}
				if( count > 0 )
					ret.add("-2 " + colix + " " + String.valueOf(kbuff._sum/count));
			}
			//pass-through constant label
			else if( _encoder.getMethod(colix) == MVMethod.CONSTANT ) {
				if( iter.hasNext() )
					ret.add("-2 " + colix + " " + iter.next().getMvValue());
			}
			
			return ret.iterator();
		}
	}
}
