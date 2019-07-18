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

package org.tugraz.sysds.hops;

import org.tugraz.sysds.common.Types.DataType;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.lops.Lop;
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.lops.TensorPartialAggregate;
import org.tugraz.sysds.runtime.meta.MatrixCharacteristics;


public class TensorAggUnaryOp extends MultiThreadedHop
{
	private AggOp _op;
	private Direction _direction;

	private TensorAggUnaryOp() {
		//default constructor for clone
	}

	public TensorAggUnaryOp(String l, DataType dt, ValueType vt, AggOp o, Direction idx, Hop inp)
	{
		super(l, dt, vt);
		_op = o;
		_direction = idx;
		getInput().add(0, inp);
		inp.getParent().add(this);
	}

	@Override
	public void checkArity() {
		HopsException.check(_input.size() == 1, this, "should have arity 1 but has arity %d", _input.size());
	}

	public AggOp getOp()
	{
		return _op;
	}

	public void setOp(AggOp op)
	{
		_op = op;
	}

	public Direction getDirection()
	{
		return _direction;
	}

	public void setDirection(Direction direction)
	{
		_direction = direction;
	}

	@Override
	public boolean isGPUEnabled() {
		// TODO GPU enable
		return false;
	}

	@Override
	public Lop constructLops()
	{
		//return already created lops
		if( getLops() != null )
			return getLops();

		try
		{
			int k = OptimizerUtils.getConstrainedNumThreads(_maxNumThreads);
			Hop input = getInput().get(0);
			// TODO use different ExecTypes and other conditions. See `AggUnaryOp`
			TensorPartialAggregate agg = new TensorPartialAggregate(input.constructLops(), HopsTensorAgg2Lops.get(_op),
					TensorPartialAggregate.DirectionTypes.RowCol, getDataType(), getValueType(), ExecType.CP, k);
			setOutputDimensions(agg);
			setLineNumbers(agg);
			setLops(agg);
		}
		catch (Exception e) {
			throw new HopsException(this.printErrorLocation() + "In TensorAggUnary Hop, error constructing Lops " , e);
		}

		//add reblock/checkpoint lops if necessary
		constructAndSetLopsDataFlowProperties();

		//return created lops
		return getLops();
	}



	@Override
	public String getOpString() {
		// TODO use different OpString?
		//tua - tensor unary aggregate, for consistency with runtime
		return "tua(" +
				HopsAgg2String.get(_op) +
				HopsDirection2String.get(_direction) + ")";
	}

	@Override
	public boolean allowsAllExecTypes() {
		// TODO allow all exectypes
		return false;
	}

	@Override
	protected double computeOutputMemEstimate( long dim1, long dim2, long nnz )
	{
		// TODO check if correct, copied from `AggUnaryOp`
		double sparsity;
		if (isGPUEnabled()) {
			// The GPU version (for the time being) only does dense outputs
			sparsity = 1.0;
		} else {
			sparsity = OptimizerUtils.getSparsity(dim1, dim2, nnz);
		}

		return OptimizerUtils.estimateSizeExactSparsity(dim1, dim2, sparsity);
	}

	@Override
	protected double computeIntermediateMemEstimate( long dim1, long dim2, long nnz )
	{
		// TODO check if correct, copied from `AggUnaryOp`
		 //default: no additional memory required
		double val = 0;
		double sparsity = OptimizerUtils.getSparsity(dim1, dim2, nnz);

		switch( _op )
		{
			case SUM:
				//worst-case correction LASTROW / LASTCOLUMN
				if( _direction == Direction.Col ) //(potentially sparse)
					val = OptimizerUtils.estimateSizeExactSparsity(2, dim2, sparsity);
				else if( _direction == Direction.Row ) //(always dense)
					val = OptimizerUtils.estimateSizeExactSparsity(dim1, 2, 1.0);
				break;
				// TODO other Aggregations
			default:
				//no intermediate memory consumption
				val = 0;
		}

		return val;
	}

	@Override
	protected long[] inferOutputCharacteristics( MemoTable memo )
	{
		// TODO check if correct, copied from `AggUnaryOp`
		long[] ret = null;

		Hop input = getInput().get(0);
		MatrixCharacteristics mc = memo.getAllInputStats(input);
		if( _direction == Direction.Col && mc.colsKnown() )
			ret = new long[]{1, mc.getCols(), -1};
		else if( _direction == Direction.Row && mc.rowsKnown() )
			ret = new long[]{mc.getRows(), 1, -1};

		return ret;
	}


	@Override
	protected ExecType optFindExecType() {
		// TODO find exectype
		return ExecType.CP;
	}

	@Override
	public void refreshSizeInformation()
	{
		if (getDataType() != DataType.SCALAR)
		{
			Hop input = getInput().get(0);
			if ( _direction == Direction.Col ) //colwise computations
			{
				setDim1(1);
				setDim2(input.getDim2());
			}
			else if ( _direction == Direction.Row )
			{
				setDim1(input.getDim1());
				setDim2(1);	
			}
		}
	}
	
	@Override
	public boolean isTransposeSafe()
	{
		return (_direction == Direction.RowCol) && _op == AggOp.SUM;
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException 
	{
		TensorAggUnaryOp ret = new TensorAggUnaryOp();
		
		//copy generic attributes
		ret.clone(this, false);
		
		//copy specific attributes
		ret._op = _op;
		ret._direction = _direction;
		ret._maxNumThreads = _maxNumThreads;
		
		return ret;
	}
	
	@Override
	public boolean compare( Hop that )
	{
		if( !(that instanceof TensorAggUnaryOp) )
			return false;
		
		TensorAggUnaryOp that2 = (TensorAggUnaryOp)that;
		return (   _op == that2._op
				&& _direction == that2._direction
				&& _maxNumThreads == that2._maxNumThreads
				&& getInput().get(0) == that2.getInput().get(0));
	}
}
