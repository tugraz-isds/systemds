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
 */
package org.tugraz.sysds.runtime.data;

import org.apache.commons.lang.NotImplementedException;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.lops.PartialAggregate;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.functionobjects.IndexFunction;
import org.tugraz.sysds.runtime.functionobjects.KahanFunction;
import org.tugraz.sysds.runtime.functionobjects.KahanPlus;
import org.tugraz.sysds.runtime.functionobjects.Mean;
import org.tugraz.sysds.runtime.functionobjects.ReduceAll;
import org.tugraz.sysds.runtime.functionobjects.ValueFunction;
import org.tugraz.sysds.runtime.instructions.cp.KahanObject;
import org.tugraz.sysds.runtime.matrix.operators.AggregateOperator;
import org.tugraz.sysds.runtime.matrix.operators.AggregateUnaryOperator;
import org.tugraz.sysds.runtime.util.CommonThreadPool;
import org.tugraz.sysds.runtime.util.UtilFunctions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

public class LibTensorAgg {
	private enum AggType {
		KAHAN_SUM,
		INVALID,
	}

	/**
	 * Check if a aggregation fulfills the constraints to be split to multiple threads.
	 *
	 * @param in the tensor block to be aggregated
	 * @param k the number of threads
	 * @return true if aggregation should be done on multiple threads, false otherwise
	 */
	public static boolean satisfiesMultiThreadingConstraints(TensorBlock in, int k) {
		// TODO more conditions depending on operation
		return k > 1 && in._vt != Types.ValueType.BOOLEAN;
	}

	/**
	 * Aggregate a tensor-block with the given unary operator on multiple threads. Might choose to do it on a single
	 * thread if certain constraints are not fulfilled (See `satisfiesMultiThreadingConstraints()`).
	 *
	 * @param in the input tensor block
	 * @param out the output tensor block
	 * @param uaop the unary operation to apply
	 * @param k the number of threads to use
	 */
	public static void aggregateUnaryTensor(TensorBlock in, TensorBlock out, AggregateUnaryOperator uaop, int k) {
		AggType aggType = getAggType(uaop);
		//fall back to sequential version if necessary
		if( !satisfiesMultiThreadingConstraints(in, k) ) {
			aggregateUnaryTensor(in, out, uaop);
			return;
		}
		// TODO filter empty input blocks (incl special handling for sparse-unsafe operations)
		if( in.isEmpty(false) ){
			aggregateUnaryTensorEmpty(in, out, aggType, uaop.indexFn);
			return;
		}
		try {
			ExecutorService pool = CommonThreadPool.get(k);
			ArrayList<AggTask> tasks = new ArrayList<>();
			ArrayList<Integer> blklens = UtilFunctions.getBalancedBlockSizesDefault(in.getDim(0), k, false);
			for( int i=0, lb=0; i<blklens.size(); lb+=blklens.get(i), i++ ) {
				tasks.add(new PartialAggTask(in, out, aggType, uaop, lb, lb+blklens.get(i)) );
			}
			pool.invokeAll(tasks);
			pool.shutdown();
			//aggregate partial results
			out.copy(((PartialAggTask)tasks.get(0)).getResult()); //for init
			for( int i=1; i<tasks.size(); i++ )
				aggregateFinalResult(uaop.aggOp, out, ((PartialAggTask)tasks.get(i)).getResult());
		}
		catch(Exception ex) {
			throw new DMLRuntimeException(ex);
		}
		// TODO change to sparse if worth it
	}

	/**
	 * Aggregate a tensor-block with the given unary operator.
	 *
	 * @param in the input tensor block
	 * @param out the output tensor block containing the aggregated result
	 * @param uaop the unary operation to apply
	 */
	public static void aggregateUnaryTensor(TensorBlock in, TensorBlock out, AggregateUnaryOperator uaop) {
		AggType aggType = getAggType(uaop);
		// TODO filter empty input blocks (incl special handling for sparse-unsafe operations)
		if( in.isEmpty(false) ){
			aggregateUnaryTensorEmpty(in, out, aggType, uaop.indexFn);
			return;
		}
		if( !in.isSparse() )
			aggregateUnaryTensorDense(in, out, aggType, uaop.aggOp.increOp.fn, uaop.indexFn, 0, in.getDim(0));
		else
			// TODO aggregation for sparse tensors
			throw new NotImplementedException("Tensor aggregation not supported for sparse tensors.");
		// TODO change to sparse if worth it
	}

	/**
	 * Aggregate a empty tensor-block with a unary operator.
	 * @param in the tensor-block to aggregate
	 * @param out the resulting tensor-block
	 * @param optype the operation to apply
	 * @param ixFn the indexFunction
	 * @return the resulting tensor-block
	 */
	private static TensorBlock aggregateUnaryTensorEmpty(TensorBlock in, TensorBlock out, AggType optype,
	                                                     IndexFunction ixFn) {
		// TODO implement for other optypes and IndexFunctions
		if( ixFn instanceof ReduceAll && (in.getNumRows() == 0 || in.getNumColumns() == 0) ) {
			double val;
			if (optype == AggType.KAHAN_SUM) {
				val = 0;
			} else {
				val = Double.NaN;
			}
			out.set(new int[]{0, 0}, val);
			return out;
		}
		return out;
	}

	/**
	 * Core incremental tensor aggregate (ak+) as used for uack+ and acrk+.
	 * Embedded correction values.
	 *
	 * @param in tensor block
	 * @param aggVal aggregate operator
	 * @param aop aggregate operator
	 */
	public static void aggregateBinaryTensor(TensorBlock in, TensorBlock aggVal, AggregateOperator aop) {
		//check validity
		if(in.getLength() != aggVal.getLength()) {
			throw new DMLRuntimeException("Binary tensor aggregation requires consistent numbers of cells (" +
					Arrays.toString(in._dims) + ", " + Arrays.toString(aggVal._dims) + ").");
		}

		//core aggregation
		boolean lastRowCorr = (aop.correctionLocation == PartialAggregate.CorrectionLocationType.LASTROW);
		boolean lastColCorr = (aop.correctionLocation == PartialAggregate.CorrectionLocationType.LASTCOLUMN);
		if( !in.isSparse() && lastRowCorr )
			aggregateBinaryTensorLastRowDenseGeneric(in, aggVal);
		else if( in.isSparse() && lastRowCorr )
			throw new DMLRuntimeException("Aggregation on sparse tensors not yet supported.");
		else if( !in.isSparse() && lastColCorr )
			aggregateBinaryTensorLastColDenseGeneric(in, aggVal);
		else if( in.isSparse() && lastColCorr )
			throw new DMLRuntimeException("Aggregation on sparse tensors not yet supported.");
	}

	/**
	 * Get the aggregation type from the unary operator.
	 *
	 * @param op the unary operator
	 * @return the aggregation type
	 */
	private static AggType getAggType(AggregateUnaryOperator op) {
		ValueFunction vfn = op.aggOp.increOp.fn;

		//(kahan) sum
		if (vfn instanceof KahanPlus)
			return AggType.KAHAN_SUM;
		return AggType.INVALID;
	}

	/**
	 * Determines whether the unary operator is supported.
	 * @param op the unary operator to check
	 * @return true if the operator is supported, false otherwise
	 */
	public static boolean isSupportedUnaryAggregateOperator( AggregateUnaryOperator op ) {
		AggType type = getAggType( op );
		return (type != AggType.INVALID);
	}

	/**
	 * Aggregate a subset of rows of a dense tensor block.
	 * @param in the tensor block to aggregate
	 * @param out the aggregation result with correction
	 * @param aggtype the type of aggregation to use
	 * @param fn the function to use
	 * @param ixFn the IndexFunction to use
	 * @param rl the lower index of rows to use
	 * @param ru the upper index of rows to use (exclusive)
	 */
	private static void aggregateUnaryTensorDense(TensorBlock in, TensorBlock out, AggType aggtype, ValueFunction fn,
	                                              IndexFunction ixFn, int rl, int ru) {
		//note: due to corrections, even the output might be a large dense block
		DenseBlock a = in.getDenseBlock();
		DenseBlock c = out.getDenseBlock();
		if (aggtype == AggType.KAHAN_SUM) {
			KahanObject kbuff = new KahanObject(0, 0);
			if (ixFn instanceof ReduceAll) // SUM
				d_uakp(a, c, kbuff, (KahanPlus) fn, rl, ru);
		}
		// TODO other aggregations
	}

	/**
	 * Add two tensor-blocks, which contain the result of an aggregation with correction in the last row, together.
	 * @param in the tensor block to add
	 * @param aggVal the tensor block to which the first should be added
	 */
	private static void aggregateBinaryTensorLastRowDenseGeneric(TensorBlock in, TensorBlock aggVal) {
		if( in._denseBlock==null || in.isEmpty(false) )
			return;

		final int m = in.getDim(0);
		final int n = in.getDim(1);
		final int cix = (m-1)*n;

		double[] a = in.getDenseBlock().valuesAt(0);

		KahanObject buffer = new KahanObject(0, 0);
		KahanPlus akplus = KahanPlus.getKahanPlusFnObject();

		//incl implicit nnz maintenance
		for(int i=0, ix=0; i<m-1; i++)
			for(int j=0; j<n; j++, ix++) {
				buffer._sum = aggVal.get(new int[]{i, j});
				buffer._correction = aggVal.get(new int[]{m - 1, j});
				akplus.execute(buffer, a[ix], a[cix + j]);
				aggVal.set(new int[]{i, j}, buffer._sum);
				aggVal.set(new int[]{m - 1, j}, buffer._correction);
			}
		// TODO check sparsity
	}

	/**
	 * Add two tensor-blocks, which contain the result of an aggregation with correction in the last column, together.
	 * @param in the tensor block to add
	 * @param aggVal the tensor block to which the first should be added
	 */
	private static void aggregateBinaryTensorLastColDenseGeneric(TensorBlock in, TensorBlock aggVal) {
		if( in._denseBlock==null || in.isEmpty(false) )
			return;

		final int m = in.getDim(0);
		final int n = in.getDim(1);

		double[] a = in.getDenseBlock().valuesAt(0);

		KahanObject buffer = new KahanObject(0, 0);
		KahanPlus akplus = KahanPlus.getKahanPlusFnObject();

		//incl implicit nnz maintenance
		for(int i=0, ix=0; i<m; i++, ix+=n)
			for(int j=0; j<n-1; j++) {
				buffer._sum = aggVal.get(new int[]{i, j});
				buffer._correction = aggVal.get(new int[]{i, n - 1});
				akplus.execute(buffer, a[ix + j], a[ix + j + 1]);
				aggVal.set(new int[]{i, j}, buffer._sum);
				aggVal.set(new int[]{i, n - 1}, buffer._correction);
			}
		// TODO check sparsity
	}

	/**
	 * Add two partial aggregations together.
	 * @param aop the aggregation operator
	 * @param out the tensor-block which contains partial result and should be increased to contain sum of both results
	 * @param partout the tensor-block which contains partial result and should be added to other partial result
	 */
	private static void aggregateFinalResult(AggregateOperator aop, TensorBlock out, TensorBlock partout ) {
		AggregateOperator laop = aop;

		//TODO special handling for mean where the final aggregate operator (kahan plus)
		// is not equals to the partial aggregate operator
		if( aop.increOp.fn instanceof Mean ) {
			laop = new AggregateOperator(0, KahanPlus.getKahanPlusFnObject(), aop.correctionExists,
					aop.correctionLocation);
		}

		//incremental aggregation of final results
		if( laop.correctionExists )
			out.incrementalAggregate(laop, partout);
		else
			throw new NotImplementedException();
			//out.binaryOperationsInPlace(laop.increOp, partout);
	}

	/**
	 * SUM, opcode: uak+, dense input.
	 *
	 * @param a input dense block
	 * @param c output dense block with correction
	 * @param kbuff kahan buffer
	 * @param kplus kahan plus object
	 * @param rl row lower index
	 * @param ru row upper index
	 */
	private static void d_uakp( DenseBlock a, DenseBlock c, KahanObject kbuff, KahanPlus kplus, int rl, int ru ) {
		final int bil = a.index(rl);
		final int biu = a.index(ru-1);
		if (a instanceof DenseBlockBool || a instanceof DenseBlockLBool) {
			kbuff._sum = a.countNonZeros(rl, ru, 0, a.getCumODims(0));
		} else if (a instanceof DenseBlockFP64 || a instanceof DenseBlockLFP64) {
			for (int bix = bil; bix <= biu; bix++) {
				double[] values = a.valuesAt(bix);
				int lpos = (bix==bil) ? a.pos(rl) : 0;
				int len = (bix==biu) ? a.pos(ru - 1) - lpos + a.getCumODims(0) : a.blockSize(bix) * a.getCumODims(0);
				sum(values, lpos, len, kbuff, kplus);
			}
		} else {
			// TODO special case String: currently converted to double, should be concatenated?
			// Less readable and makes use of how row-column-getter actually works, but is a lot faster than
			// using the index[]-getter.
			long sum = 0;
			for (int bix = bil; bix <= biu; bix++) {
				int lpos = (bix == bil) ? a.pos(rl) : 0;
				int len = (bix == biu) ? a.pos(ru - 1) - lpos + a.getCumODims(0) : a.blockSize(bix) * a.getCumODims(0);
				for (int i = 0; i < len; i++)
					sum += UtilFunctions.toLong(a.get(bix * a.blockSize() * a.getCumODims(0), lpos + i));
			}
			kbuff._sum = sum;
		}
		c.set(kbuff);
	}

	/**
	 * Sum an array with kahan
	 * @param a the array to sum
	 * @param ai the starting to index
	 * @param len number of elements to sum
	 * @param kbuff the kahan buffer object
	 * @param kplus the kahan function
	 */
	private static void sum(double[] a, int ai, final int len, KahanObject kbuff, KahanFunction kplus) {
		for (int i=ai; i<ai+len; i++)
			kplus.execute2(kbuff, a[i]);
	}

	// TODO maybe merge this, and other parts, with `LibMatrixAgg`
	private static abstract class AggTask implements Callable<Object> {}

	private static class PartialAggTask extends AggTask
	{
		private TensorBlock _in;
		private TensorBlock _ret;
		private AggType _aggtype;
		private AggregateUnaryOperator _uaop;
		private int _rl;
		private int _ru;

		protected PartialAggTask( TensorBlock in, TensorBlock ret, AggType aggtype, AggregateUnaryOperator uaop, int rl,
		                          int ru ) {
			_in = in;
			_ret = ret;
			_aggtype = aggtype;
			_uaop = uaop;
			_rl = rl;
			_ru = ru;
		}

		@Override
		public Object call() {
			//thead-local allocation for partial aggregation
			// _ret should always have exactly two dimensions, sum value and correction value as a vector
			_ret = new TensorBlock(_ret._vt, new int[]{_ret.getDim(0), _ret.getDim(1)});
			_ret.allocateDenseBlock();

			if( !_in.isSparse() )
				aggregateUnaryTensorDense(_in, _ret, _aggtype, _uaop.aggOp.increOp.fn, _uaop.indexFn, _rl, _ru);
			else
				throw new DMLRuntimeException("Sparse aggregation not implemented for Tensor");

			//TODO recompute non-zeros of partial result
			return null;
		}

		public TensorBlock getResult() {
			return _ret;
		}
	}
}
