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

package org.tugraz.sysds.runtime.instructions.spark.functions;

import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.tugraz.sysds.lops.BinaryM.VectorType;
import org.tugraz.sysds.runtime.instructions.spark.data.LazyIterableIterator;
import org.tugraz.sysds.runtime.instructions.spark.data.PartitionedBroadcast;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.data.MatrixIndexes;
import org.tugraz.sysds.runtime.matrix.operators.BinaryOperator;

import scala.Tuple2;

public class MatrixVectorBinaryOpPartitionFunction implements PairFlatMapFunction<Iterator<Tuple2<MatrixIndexes,MatrixBlock>>, MatrixIndexes,MatrixBlock>
{
	private static final long serialVersionUID = 9096091404578628534L;
	
	private BinaryOperator _op = null;
	private PartitionedBroadcast<MatrixBlock> _pmV = null;
	private VectorType _vtype = null;
	
	public MatrixVectorBinaryOpPartitionFunction( BinaryOperator op, PartitionedBroadcast<MatrixBlock> binput, VectorType vtype ) 
	{
		_op = op;
		_pmV = binput;
		_vtype = vtype;
	}

	@Override
	public LazyIterableIterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> arg0) 
		throws Exception 
	{
		return new MapBinaryPartitionIterator( arg0 );
	}
	
	/**
	 * Lazy mbinary iterator to prevent materialization of entire partition output in-memory.
	 * The implementation via mapPartitions is required to preserve partitioning information,
	 * which is important for performance. 
	 */
	private class MapBinaryPartitionIterator extends LazyIterableIterator<Tuple2<MatrixIndexes, MatrixBlock>>
	{
		public MapBinaryPartitionIterator(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> in) {
			super(in);
		}

		@Override
		protected Tuple2<MatrixIndexes, MatrixBlock> computeNext(Tuple2<MatrixIndexes, MatrixBlock> arg)
			throws Exception
		{
			//unpack partition key-value pairs
			MatrixIndexes ix = arg._1();
			MatrixBlock in1 = arg._2();
			
			//get the rhs block 
			int rix= (int)((_vtype==VectorType.COL_VECTOR) ? ix.getRowIndex() : 1);
			int cix= (int)((_vtype==VectorType.COL_VECTOR) ? 1 : ix.getColumnIndex());
			MatrixBlock in2 = _pmV.getBlock(rix, cix);
			
			//execute the binary operation
			MatrixBlock ret = in1.binaryOperations (_op, in2, new MatrixBlock());
			return new Tuple2<>(ix, ret);
		}
	}
}
