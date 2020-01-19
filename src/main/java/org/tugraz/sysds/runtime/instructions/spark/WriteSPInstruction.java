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

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.LongAccumulator;
import org.tugraz.sysds.common.Types.DataType;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.tugraz.sysds.runtime.instructions.InstructionUtils;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;
import org.tugraz.sysds.runtime.instructions.spark.functions.ComputeBinaryBlockNnzFunction;
import org.tugraz.sysds.runtime.instructions.spark.utils.FrameRDDConverterUtils;
import org.tugraz.sysds.runtime.instructions.spark.utils.FrameRDDConverterUtils.LongFrameToLongWritableFrameFunction;
import org.tugraz.sysds.runtime.instructions.spark.utils.RDDConverterUtils;
import org.tugraz.sysds.runtime.io.FileFormatProperties;
import org.tugraz.sysds.runtime.io.FileFormatPropertiesCSV;
import org.tugraz.sysds.runtime.lineage.LineageItem;
import org.tugraz.sysds.runtime.lineage.LineageItemUtils;
import org.tugraz.sysds.runtime.lineage.LineageTraceable;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.data.MatrixIndexes;
import org.tugraz.sysds.runtime.matrix.data.OutputInfo;
import org.tugraz.sysds.runtime.meta.DataCharacteristics;
import org.tugraz.sysds.runtime.util.HDFSTool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class WriteSPInstruction extends SPInstruction implements LineageTraceable {
	private CPOperand input1 = null;
	private CPOperand input2 = null;
	private CPOperand input3 = null;
	private CPOperand input4 = null;
	private FileFormatProperties formatProperties;

	private WriteSPInstruction(CPOperand in1, CPOperand in2, CPOperand in3, String opcode, String str) {
		super(SPType.Write, opcode, str);
		input1 = in1;
		input2 = in2;
		input3 = in3;
		formatProperties = null; // set in case of csv
	}

	public static WriteSPInstruction parseInstruction ( String str ) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType ( str );
		String opcode = parts[0];
		
		if( !opcode.equals("write") ) {
			throw new DMLRuntimeException("Unsupported opcode");
		}
		
		// All write instructions have 3 parameters, except in case of delimited/csv file.
		// Write instructions for csv files also include three additional parameters (hasHeader, delimiter, sparse)
		if ( parts.length != 5 && parts.length != 9 ) {
			throw new DMLRuntimeException("Invalid number of operands in write instruction: " + str);
		}
		
		//SPARK°write°_mVar2·MATRIX·DOUBLE°./src/test/scripts/functions/data/out/B·SCALAR·STRING·true°matrixmarket·SCALAR·STRING·true
		// _mVar2·MATRIX·DOUBLE
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand in2 = new CPOperand(parts[2]);
		CPOperand in3 = new CPOperand(parts[3]);
		
		WriteSPInstruction inst = new WriteSPInstruction(in1, in2, in3, opcode, str); 
		
		if ( in3.getName().equalsIgnoreCase("csv") ) {
			boolean hasHeader = Boolean.parseBoolean(parts[4]);
			String delim = parts[5];
			boolean sparse = Boolean.parseBoolean(parts[6]);
			FileFormatProperties formatProperties = new FileFormatPropertiesCSV(hasHeader, delim, sparse);
			inst.setFormatProperties(formatProperties);
			CPOperand in4 = new CPOperand(parts[8]);
			inst.input4 = in4;
		} else {
			FileFormatProperties ffp = new FileFormatProperties();

			CPOperand in4 = new CPOperand(parts[4]);
			inst.input4 = in4;
			inst.setFormatProperties(ffp);
		}
		return inst;		
	}
	
	
	public FileFormatProperties getFormatProperties() {
		return formatProperties;
	}
	
	public void setFormatProperties(FileFormatProperties prop) {
		formatProperties = prop;
	}
	
	public CPOperand getInput1() { return input1; }
	
	public CPOperand getInput2() {
		return input2;
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) {
		SparkExecutionContext sec = (SparkExecutionContext) ec;

		//get filename (literal or variable expression)
		String fname = ec.getScalarInput(input2.getName(), ValueType.STRING, input2.isLiteral()).getStringValue();
		String desc = ec.getScalarInput(input4.getName(), ValueType.STRING, input4.isLiteral()).getStringValue();
		formatProperties.setDescription(desc);

		ValueType[] schema = (input1.getDataType()==DataType.FRAME) ? 
				sec.getFrameObject(input1.getName()).getSchema() : null;
		
		try
		{
			//if the file already exists on HDFS, remove it.
			HDFSTool.deleteFileIfExistOnHDFS( fname );

			//prepare output info according to meta data
			String outFmt = input3.getName();
			OutputInfo oi = OutputInfo.stringToOutputInfo(outFmt);
				
			//core matrix/frame write
			if( input1.getDataType()==DataType.MATRIX )
				processMatrixWriteInstruction(sec, fname, oi);
			else
				processFrameWriteInstruction(sec, fname, oi, schema);
		}
		catch(IOException ex)
		{
			throw new DMLRuntimeException("Failed to process write instruction", ex);
		}
	}

	protected void processMatrixWriteInstruction(SparkExecutionContext sec, String fname, OutputInfo oi) 
		throws IOException
	{
		//get input rdd
		JavaPairRDD<MatrixIndexes,MatrixBlock> in1 = sec.getBinaryMatrixBlockRDDHandleForVariable( input1.getName() );
		DataCharacteristics mc = sec.getDataCharacteristics(input1.getName());
		
		if(    oi == OutputInfo.MatrixMarketOutputInfo
			|| oi == OutputInfo.TextCellOutputInfo     ) 
		{
			//piggyback nnz maintenance on write
			LongAccumulator aNnz = null;
			if( !mc.nnzKnown() ) {
				aNnz = sec.getSparkContext().sc().longAccumulator("nnz");
				in1 = in1.mapValues(new ComputeBinaryBlockNnzFunction(aNnz));
			}
			
			JavaRDD<String> header = null;
			if( oi == OutputInfo.MatrixMarketOutputInfo  ) {
				ArrayList<String> headerContainer = new ArrayList<>(1);
				// First output MM header
				String headerStr = "%%MatrixMarket matrix coordinate real general\n" +
						// output number of rows, number of columns and number of nnz
						mc.getRows() + " " + mc.getCols() + " " + mc.getNonZeros();
				headerContainer.add(headerStr);
				header = sec.getSparkContext().parallelize(headerContainer);
			}
			
			JavaRDD<String> ijv = RDDConverterUtils.binaryBlockToTextCell(in1, mc);
			if(header != null)
				customSaveTextFile(header.union(ijv), fname, true);
			else
				customSaveTextFile(ijv, fname, false);
			
			if( !mc.nnzKnown() )
				mc.setNonZeros( aNnz.value() );
		}
		else if( oi == OutputInfo.CSVOutputInfo ) 
		{
			if( mc.getRows() == 0 || mc.getCols() == 0 ) {
				throw new IOException("Write of matrices with zero rows or columns"
					+ " not supported ("+mc.getRows()+"x"+mc.getCols()+").");
			}
			
			LongAccumulator aNnz = null;
			
			//piggyback nnz computation on actual write
			if( !mc.nnzKnown() ) {
				aNnz = sec.getSparkContext().sc().longAccumulator("nnz");
				in1 = in1.mapValues(new ComputeBinaryBlockNnzFunction(aNnz));
			}	
			
			JavaRDD<String> out = RDDConverterUtils.binaryBlockToCsv(
				in1, mc, (FileFormatPropertiesCSV) formatProperties, true);

			customSaveTextFile(out, fname, false);
			
			if( !mc.nnzKnown() )
				mc.setNonZeros(aNnz.value().longValue());
		}
		else if( oi == OutputInfo.BinaryBlockOutputInfo ) {
			//piggyback nnz computation on actual write
			LongAccumulator aNnz = null;
			if( !mc.nnzKnown() ) {
				aNnz = sec.getSparkContext().sc().longAccumulator("nnz");
				in1 = in1.mapValues(new ComputeBinaryBlockNnzFunction(aNnz));
			}
			
			//save binary block rdd on hdfs
			in1.saveAsHadoopFile(fname, MatrixIndexes.class, MatrixBlock.class, SequenceFileOutputFormat.class);
			
			if( !mc.nnzKnown() )
				mc.setNonZeros(aNnz.value().longValue());
		}
		else {
			//unsupported formats: binarycell (not externalized)
			throw new DMLRuntimeException("Unexpected data format: " + OutputInfo.outputInfoToString(oi));
		}
		
		// write meta data file
		HDFSTool.writeMetaDataFile (fname + ".mtd", ValueType.FP64, mc, oi, formatProperties);
	}

	protected void processFrameWriteInstruction(SparkExecutionContext sec, String fname, OutputInfo oi, ValueType[] schema) 
		throws IOException
	{
		//get input rdd
		JavaPairRDD<Long,FrameBlock> in1 = sec
			.getFrameBinaryBlockRDDHandleForVariable(input1.getName());
		DataCharacteristics mc = sec.getDataCharacteristics(input1.getName());
		
		if( oi == OutputInfo.TextCellOutputInfo ) {
			JavaRDD<String> out = FrameRDDConverterUtils.binaryBlockToTextCell(in1, mc);
			customSaveTextFile(out, fname, false);
		}
		else if( oi == OutputInfo.CSVOutputInfo ) {
			FileFormatPropertiesCSV props = (formatProperties!=null) ?(FileFormatPropertiesCSV) formatProperties : null;
			JavaRDD<String> out = FrameRDDConverterUtils.binaryBlockToCsv(in1, mc, props, true);
			customSaveTextFile(out, fname, false);
		}
		else if( oi == OutputInfo.BinaryBlockOutputInfo ) {
			JavaPairRDD<LongWritable,FrameBlock> out = in1.mapToPair(new LongFrameToLongWritableFrameFunction());
			out.saveAsHadoopFile(fname, LongWritable.class, FrameBlock.class, SequenceFileOutputFormat.class);
		}
		else {
			//unsupported formats: binarycell (not externalized)
			throw new DMLRuntimeException("Unexpected data format: " + OutputInfo.outputInfoToString(oi));
		}
		
		// write meta data file
		HDFSTool.writeMetaDataFile(fname + ".mtd", input1.getValueType(), schema, DataType.FRAME, mc, oi, formatProperties);	
	}

	private static void customSaveTextFile(JavaRDD<String> rdd, String fname, boolean inSingleFile) {
		if(inSingleFile) {
			Random rand = new Random();
			String randFName = fname + "_" + rand.nextLong() + "_" + rand.nextLong();
			try {
				while(HDFSTool.existsFileOnHDFS(randFName)) {
					randFName = fname + "_" + rand.nextLong() + "_" + rand.nextLong();
				}
				
				rdd.saveAsTextFile(randFName);
				HDFSTool.mergeIntoSingleFile(randFName, fname); // Faster version :)
				
				// rdd.coalesce(1, true).saveAsTextFile(randFName);
				// MapReduceTool.copyFileOnHDFS(randFName + "/part-00000", fname);
			} catch (IOException e) {
				throw new DMLRuntimeException("Cannot merge the output into single file: " + e.getMessage());
			}
			finally {
				try {
					// This is to make sure that we donot create random files on HDFS
					HDFSTool.deleteFileIfExistOnHDFS( randFName );
				} catch (IOException e) {
					throw new DMLRuntimeException("Cannot merge the output into single file: " + e.getMessage());
				}
			}
		}
		else {
			rdd.saveAsTextFile(fname);
		}
	}

	@Override
	public LineageItem[] getLineageItems(ExecutionContext ec) {
		LineageItem[] ret = LineageItemUtils.getLineage(ec, input1, input2, input3, input4);
		if (formatProperties != null && formatProperties.getDescription() != null && !formatProperties.getDescription().isEmpty())
			ret = (LineageItem[])ArrayUtils.add(ret, new LineageItem(formatProperties.getDescription()));
		return new LineageItem[]{new LineageItem(input1.getName(), getOpcode(), ret)};
	}
}
