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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.tugraz.sysds.common.Types.CorrectionLocationType;
import org.tugraz.sysds.lops.UAggOuterChain;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.tugraz.sysds.runtime.functionobjects.Builtin;
import org.tugraz.sysds.runtime.functionobjects.IndexFunction;
import org.tugraz.sysds.runtime.functionobjects.ReduceAll;
import org.tugraz.sysds.runtime.functionobjects.ReduceCol;
import org.tugraz.sysds.runtime.functionobjects.ReduceRow;
import org.tugraz.sysds.runtime.instructions.InstructionUtils;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;
import org.tugraz.sysds.runtime.instructions.spark.data.LazyIterableIterator;
import org.tugraz.sysds.runtime.instructions.spark.data.PartitionedBroadcast;
import org.tugraz.sysds.runtime.instructions.spark.functions.AggregateDropCorrectionFunction;
import org.tugraz.sysds.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.tugraz.sysds.runtime.matrix.data.LibMatrixOuterAgg;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.data.MatrixIndexes;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue;
import org.tugraz.sysds.runtime.matrix.data.OperationsOnMatrixValues;
import org.tugraz.sysds.runtime.matrix.operators.AggregateOperator;
import org.tugraz.sysds.runtime.matrix.operators.AggregateUnaryOperator;
import org.tugraz.sysds.runtime.matrix.operators.BinaryOperator;
import org.tugraz.sysds.runtime.meta.DataCharacteristics;
import org.tugraz.sysds.runtime.util.DataConverter;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Two types of broadcast variables used -- 1. Array of type double. 2.PartitionedMatrixBlock
 * 1. Array of type double: Matrix B is sorted at driver level and passed to every task for cases where operations are handled with special cases. e.g. &lt;, RowSum
 * 2. PartitionedMatrixBlock:  Any operations not implemented through this change goes through generic process, In that case, task takes Matrix B, in partitioned form and operate on it.
 */
public class UaggOuterChainSPInstruction extends BinarySPInstruction {
	// operators
	private AggregateUnaryOperator _uaggOp = null;
	private AggregateOperator _aggOp = null;
	private BinaryOperator _bOp = null;

	private UaggOuterChainSPInstruction(BinaryOperator bop, AggregateUnaryOperator uaggop, AggregateOperator aggop,
			CPOperand in1, CPOperand in2, CPOperand out, String opcode, String istr) {
		super(SPType.UaggOuterChain, bop, in1, in2, out, opcode, istr);
		_uaggOp = uaggop;
		_aggOp = aggop;
		_bOp = bop;
		instString = istr;
	}

	public static UaggOuterChainSPInstruction parseInstruction( String str ) {
		String parts[] = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];

		if ( opcode.equalsIgnoreCase(UAggOuterChain.OPCODE)) {
			
			AggregateUnaryOperator uaggop = InstructionUtils.parseBasicAggregateUnaryOperator(parts[1]);
			BinaryOperator bop = InstructionUtils.parseBinaryOperator(parts[2]);

			CPOperand in1 = new CPOperand(parts[3]);
			CPOperand in2 = new CPOperand(parts[4]);
			CPOperand out = new CPOperand(parts[5]);
					
			//derive aggregation operator from unary operator
			String aopcode = InstructionUtils.deriveAggregateOperatorOpcode(parts[1]);
			CorrectionLocationType corrLoc = InstructionUtils.deriveAggregateOperatorCorrectionLocation(parts[1]);
			AggregateOperator aop = InstructionUtils.parseAggregateOperator(aopcode, corrLoc.toString());

			return new UaggOuterChainSPInstruction(bop, uaggop, aop, in1, in2, out, opcode, str);
		} 
		else {
			throw new DMLRuntimeException("UaggOuterChainSPInstruction.parseInstruction():: Unknown opcode " + opcode);
		}

	}
	
	@Override
	public void processInstruction(ExecutionContext ec) {
		SparkExecutionContext sec = (SparkExecutionContext)ec;
		
		boolean rightCached = (_uaggOp.indexFn instanceof ReduceCol || _uaggOp.indexFn instanceof ReduceAll
				              || !LibMatrixOuterAgg.isSupportedUaggOp(_uaggOp, _bOp));
		String rddVar = (rightCached) ? input1.getName() : input2.getName();
		String bcastVar = (rightCached) ? input2.getName() : input1.getName();

		//get rdd input
		JavaPairRDD<MatrixIndexes,MatrixBlock> in1 = sec.getBinaryMatrixBlockRDDHandleForVariable( rddVar );
		DataCharacteristics mcIn = sec.getDataCharacteristics(rddVar);
		boolean noKeyChange = preservesPartitioning(mcIn, _uaggOp.indexFn); 
		
		//execute UAggOuterChain instruction
		JavaPairRDD<MatrixIndexes,MatrixBlock> out = null;

		if (LibMatrixOuterAgg.isSupportedUaggOp(_uaggOp, _bOp))
		{
			//create sorted broadcast matrix 
			MatrixBlock mb = sec.getMatrixInput(bcastVar);
			sec.releaseMatrixInput(bcastVar);
			bcastVar = null; //prevent lineage tracking
			double[] vmb = DataConverter.convertToDoubleVector(mb);
			Broadcast<int[]> bvi = null;
			
			if(_uaggOp.aggOp.increOp.fn instanceof Builtin) {
				int[] vix = LibMatrixOuterAgg.prepareRowIndices(mb.getNumColumns(), vmb, _bOp, _uaggOp);
				bvi = sec.getSparkContext().broadcast(vix);
			} else
				Arrays.sort(vmb);
			
			Broadcast<double[]> bv = sec.getSparkContext().broadcast(vmb);
			
			//partitioning-preserving map-to-pair (under constraints)
			out = in1.mapPartitionsToPair( new RDDMapUAggOuterChainFunction(bv, bvi, _bOp, _uaggOp), noKeyChange );
		}
		else
		{
			PartitionedBroadcast<MatrixBlock> bv = sec.getBroadcastForVariable( bcastVar ); 
			
			//partitioning-preserving map-to-pair (under constraints)
			out = in1.mapPartitionsToPair( new RDDMapGenUAggOuterChainFunction(bv, _uaggOp, _aggOp, _bOp, mcIn), noKeyChange );	
		}
		
		//final aggregation if required
		if(_uaggOp.indexFn instanceof ReduceAll ) //RC AGG (output is scalar)
		{
			MatrixBlock tmp = RDDAggregateUtils.aggStable(out, _aggOp);
			
			//drop correction after aggregation
			tmp.dropLastRowsOrColumns(_aggOp.correction);

			//put output block into symbol table (no lineage because single block)
			sec.setMatrixOutput(output.getName(), tmp);
		}
		else //R/C AGG (output is rdd)
		{
			//put output RDD handle into symbol table
			updateUnaryAggOutputDataCharacteristics(sec);
			
			if( _uaggOp.aggOp.existsCorrection() )
				out = out.mapValues( new AggregateDropCorrectionFunction(_uaggOp.aggOp) );
			
			sec.setRDDHandleForVariable(output.getName(), out);
			sec.addLineageRDD(output.getName(), rddVar);
			if( bcastVar != null )
				sec.addLineageBroadcast(output.getName(), bcastVar);
		}
	}

	protected static boolean preservesPartitioning(DataCharacteristics mcIn, IndexFunction ixfun )
	{
		if( ixfun instanceof ReduceCol ) //rowSums
			return mcIn.dimsKnown() && mcIn.getCols() <= mcIn.getBlocksize();
		else // colsSums
			return mcIn.dimsKnown() && mcIn.getRows() <= mcIn.getBlocksize();
	}

	protected void updateUnaryAggOutputDataCharacteristics(SparkExecutionContext sec) {
		String strInput1Name, strInput2Name;
		if(_uaggOp.indexFn instanceof ReduceCol) {
			strInput1Name = input1.getName();
			strInput2Name = input2.getName();
		} else {
			strInput1Name = input2.getName();
			strInput2Name = input1.getName();
		}
		DataCharacteristics mc1 = sec.getDataCharacteristics(strInput1Name);
		DataCharacteristics mc2 = sec.getDataCharacteristics(strInput2Name);
		DataCharacteristics mcOut = sec.getDataCharacteristics(output.getName());
	
		if(!mcOut.dimsKnown()) {
			if(!mc1.dimsKnown()) {
				throw new DMLRuntimeException("The output dimensions are not specified and cannot be inferred from input:" + mc1.toString() + " " + mcOut.toString());
			}
			else {
				//infer statistics from input based on operator
				if( _uaggOp.indexFn instanceof ReduceAll )
					mcOut.set(1, 1, mc1.getBlocksize());
				else if (_uaggOp.indexFn instanceof ReduceCol)
					mcOut.set(mc1.getRows(), 1, mc1.getBlocksize());
				else if (_uaggOp.indexFn instanceof ReduceRow)
					mcOut.set(1, mc2.getCols(), mc1.getBlocksize());
			}
		}
	}

	private static class RDDMapUAggOuterChainFunction implements PairFlatMapFunction<Iterator<Tuple2<MatrixIndexes, MatrixBlock>>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = 8197406787010296291L;

		private Broadcast<double[]> _bv = null;
		private Broadcast<int[]> _bvi = null;
		private BinaryOperator _bOp = null;
		private AggregateUnaryOperator _uaggOp = null;
		
		public RDDMapUAggOuterChainFunction(Broadcast<double[]> bv, Broadcast<int[]> bvi, BinaryOperator bOp, AggregateUnaryOperator uaggOp)
		{
			// Do not get data from BroadCast variables here, as it will try to deserialize the data whenever it gets instantiated through driver class. This will cause unnecessary delay in iinstantiating class
			// through driver, and overall process.
			// Instead of this let task gets data from BroadCast variable whenever required, as data is already available in memory for task to fetch.

			//Sorted array
			_bv = bv;
			_bvi = bvi;
			_bOp = bOp;
			_uaggOp = uaggOp;
			
		}
		
		@Override
		public LazyIterableIterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> arg0)
			throws Exception 
		{
			return new RDDMapUAggOuterChainIterator(arg0);
		}
		
		private class RDDMapUAggOuterChainIterator extends LazyIterableIterator<Tuple2<MatrixIndexes, MatrixBlock>>
		{
			public RDDMapUAggOuterChainIterator(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> in) {
				super(in);
			}

			@Override
			protected Tuple2<MatrixIndexes, MatrixBlock> computeNext(Tuple2<MatrixIndexes, MatrixBlock> arg)
				throws Exception
			{
				MatrixIndexes in1Ix = arg._1();
				MatrixBlock in1Val  = arg._2();
				MatrixIndexes outIx = new MatrixIndexes();
				MatrixBlock outVal = new MatrixBlock();
				
				int [] bvi = null;
				if((LibMatrixOuterAgg.isRowIndexMax(_uaggOp)) || (LibMatrixOuterAgg.isRowIndexMin(_uaggOp)))
					bvi = _bvi.getValue();

				LibMatrixOuterAgg.resetOutputMatrix(in1Ix, in1Val, outIx, outVal, _uaggOp);
				LibMatrixOuterAgg.aggregateMatrix(in1Val, outVal, _bv.value(), bvi, _bOp, _uaggOp);
				
				return new Tuple2<>(outIx, outVal);
			}
		}
	}

	private static class RDDMapGenUAggOuterChainFunction implements PairFlatMapFunction<Iterator<Tuple2<MatrixIndexes, MatrixBlock>>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = 8197406787010296291L;

		private PartitionedBroadcast<MatrixBlock> _pbc = null;
		
		// Operators
		private AggregateUnaryOperator _uaggOp = null;
		private AggregateOperator _aggOp = null;
		private BinaryOperator _bOp = null;

		private int _blen;
		
		//reused intermediates  
		private MatrixValue _tmpVal1 = null;
		private MatrixValue _tmpVal2 = null;

		public RDDMapGenUAggOuterChainFunction(PartitionedBroadcast<MatrixBlock> binput, AggregateUnaryOperator uaggOp, AggregateOperator aggOp, BinaryOperator bOp, 
				DataCharacteristics mc)
		{
			_pbc = binput;
			
			// Operators
			_uaggOp = uaggOp;
			_aggOp = aggOp;
			_bOp = bOp;
			
			//Matrix dimension (row, column)
			_blen = mc.getBlocksize();
			
			_tmpVal1 = new MatrixBlock();
			_tmpVal2 = new MatrixBlock();
		}
		
		@Override
		public LazyIterableIterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> arg)
			throws Exception 
		{
			return new RDDMapGenUAggOuterChainIterator(arg);
		}	
		
		private class RDDMapGenUAggOuterChainIterator extends LazyIterableIterator<Tuple2<MatrixIndexes, MatrixBlock>>
		{
			public RDDMapGenUAggOuterChainIterator(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> in) {
				super(in);
			}

			@Override
			protected Tuple2<MatrixIndexes, MatrixBlock> computeNext(Tuple2<MatrixIndexes, MatrixBlock> arg)
				throws Exception
			{
				MatrixIndexes in1Ix = arg._1();
				MatrixBlock in1Val  = arg._2();

				MatrixIndexes outIx = new MatrixIndexes();
				MatrixBlock outVal = new MatrixBlock();
				MatrixBlock corr = null;
				
					
				long  in2_colBlocks = _pbc.getNumColumnBlocks();
				
				for(int bidx=1; bidx <= in2_colBlocks; bidx++) 
				{
					MatrixValue in2Val = _pbc.getBlock(1, bidx);
					
					//outer block operation
					OperationsOnMatrixValues.performBinaryIgnoreIndexes(in1Val, in2Val, _tmpVal1, _bOp);
						
					//unary aggregate operation
					OperationsOnMatrixValues.performAggregateUnary( in1Ix, _tmpVal1, outIx, _tmpVal2, _uaggOp, _blen);
					
					//aggregate over all rhs blocks
					if( corr == null ) {
						outVal.reset(_tmpVal2.getNumRows(), _tmpVal2.getNumColumns(), false);
						corr = new MatrixBlock(_tmpVal2.getNumRows(), _tmpVal2.getNumColumns(), false);
					}
					
					if(_aggOp.existsCorrection())
						OperationsOnMatrixValues.incrementalAggregation(outVal, corr, _tmpVal2, _aggOp, true);
					else 
						OperationsOnMatrixValues.incrementalAggregation(outVal, null, _tmpVal2, _aggOp, true);
				}
				return new Tuple2<>(outIx, outVal);
			}
		}
	}
}
