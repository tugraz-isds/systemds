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
import org.tugraz.sysds.runtime.instructions.spark.data.PartitionedBroadcast;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.data.MatrixIndexes;
import org.tugraz.sysds.runtime.matrix.operators.BinaryOperator;

import scala.Tuple2;

public class OuterVectorBinaryOpFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes,MatrixBlock>, MatrixIndexes,MatrixBlock>
{
	private static final long serialVersionUID = 1730704346934726826L;
	
	private BinaryOperator _op;
	private PartitionedBroadcast<MatrixBlock> _pmV;
	
	public OuterVectorBinaryOpFunction( BinaryOperator op, PartitionedBroadcast<MatrixBlock> binput ) 
	{
		_op = op;
		_pmV = binput;
	}

	@Override
	public OuterVectorBinaryOpIterator call(Tuple2<MatrixIndexes, MatrixBlock> arg0) 
		throws Exception 
	{
		return new OuterVectorBinaryOpIterator(arg0);
	}

	private class OuterVectorBinaryOpIterator implements Iterable<Tuple2<MatrixIndexes, MatrixBlock>>, Iterator<Tuple2<MatrixIndexes, MatrixBlock>>
	{
		private Tuple2<MatrixIndexes, MatrixBlock> _currBlk = null;
		private int _currPos = -1;
		
		public OuterVectorBinaryOpIterator(Tuple2<MatrixIndexes, MatrixBlock> in) {
			_currBlk = in;
			_currPos = 1;
		}

		@Override
		public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> iterator() {
			return this;
		}

		@Override
		public boolean hasNext() {
			return (_currBlk != null 
				&& _currPos <= _pmV.getNumColumnBlocks());
		}
		
		@Override
		public Tuple2<MatrixIndexes, MatrixBlock> next() {
			Tuple2<MatrixIndexes,MatrixBlock> ret = null;
			try {
				//produce next output tuple
				MatrixIndexes ix = _currBlk._1();
				MatrixBlock in1 = _currBlk._2();
				
				MatrixBlock in2 = _pmV.getBlock(1, _currPos);
				MatrixBlock resultBlk = in1.binaryOperations (_op, in2, new MatrixBlock());
				resultBlk.examSparsity(); 
				ret = new Tuple2<>(new MatrixIndexes(ix.getRowIndex(), _currPos), resultBlk);
				_currPos ++;
			}
			catch(Exception ex) {
				throw new RuntimeException(ex);
			}
			return ret;
		}

		@Override
		public void remove() {
			throw new RuntimeException("Unsupported remove operation.");
		}
	}
}
