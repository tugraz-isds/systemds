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
import org.tugraz.sysds.lops.TensorGen;
import org.tugraz.sysds.parser.DataIdentifier;

import java.util.HashMap;
import java.util.Map.Entry;

public class TensorGenOp extends MultiThreadedHop
{
	private TensorGenOp() {
		//default constructor for clone
	}

	public TensorGenOp(DataIdentifier id, HashMap<String, Hop> inputParameters)
	{
		super(id.getName(), DataType.TENSOR, ValueType.FP64);

		for( Entry<String, Hop> e: inputParameters.entrySet() ) {
			Hop input = e.getValue();
			getInput().add(input);
			input.getParent().add(this);
		}
		//compute unknown dims and nnz
		refreshSizeInformation();
	}

	@Override
	public void checkArity() {
		int sz = _input.size();
		HopsException.check(sz == 3, this, "should have arity 3 but has arity %d", sz);
	}

	@Override
	public String getOpString() {
		return "tensorGen";
	}
	
	@Override
	public boolean isGPUEnabled() {
		return false;
	}
	
	@Override
	public Lop constructLops() 
	{
		//return already created lops
		if( getLops() != null )
			return getLops();

		// TODO other exec types
		ExecType et = optFindExecType();

		Lop[] linputs = new Lop[3]; //data, dims, byrow
		for( int i=0; i<linputs.length; i++ )
			linputs[i] = getInput().get(i).constructLops();

		_outputEmptyBlocks = (et==ExecType.SPARK &&
				!OptimizerUtils.allowsToFilterEmptyBlockOutputs(this));
		// TODO multiple ValueTypes
		// TODO think about branching here depending on inputs, at the moment interpreting the parameters, for example
		//  dims which can be given as a string, matrix or tensor, is done at instruction level
		TensorGen tensorGen = new TensorGen(linputs, ValueType.FP64, et);
		setOutputDimensions(tensorGen);
		setLineNumbers(tensorGen);
		setLops(tensorGen);
		//add reblock/checkpoint lops if necessary
		constructAndSetLopsDataFlowProperties();
		return getLops();
	}

	@Override
	public boolean allowsAllExecTypes() {
		return true;
	}	
	
	@Override
	protected double computeOutputMemEstimate( long dim1, long dim2, long nnz ) {
		double sparsity = OptimizerUtils.getSparsity(dim1, dim2, nnz);
		return OptimizerUtils.estimateSizeExactSparsity(dim1, dim2, sparsity);
	}
	
	@Override
	protected double computeIntermediateMemEstimate( long dim1, long dim2, long nnz ) {
		return 0;
	}
	
	@Override
	protected long[] inferOutputCharacteristics( MemoTable memo ) {
		return null;
	}

	@Override
	protected ExecType optFindExecType() {
		checkAndSetForcedPlatform();
		
		if( _etypeForced != null )
			_etype = _etypeForced;
		else 
		{
			if ( OptimizerUtils.isMemoryBasedOptLevel() ) {
				_etype = findExecTypeByMemEstimate();
			}
			else if (this.areDimsBelowThreshold() || this.isVector())
				_etype = ExecType.CP;
			else
				_etype = ExecType.SPARK;
		
			//check for valid CP dimensions and matrix size
			checkAndSetInvalidCPDimsAndSize();
		}

		//mark for recompile (forever)
		setRequiresRecompileIfNecessary();
		
		return _etype;
	}
	
	@Override
	public void refreshSizeInformation() {
		Hop input1 = getInput().get(0);
		Hop input2 = getInput().get(1); // dims
		// TODO get size from dims
		//refreshRowsParameterInformation(?); //refresh rows
		//refreshColsParameterInformation(?); //refresh cols
		// TODO calculate Nnz and maybe infer dims
		/*setNnz(input1.getNnz());
		if( !dimsKnown() && input1.dimsKnown() ) { //reshape allows to infer dims, if input and 1 dim known
			if(_dim1 > 0)
				_dim2 = (input1._dim1*input1._dim2)/_dim1;
			else if(_dim2 > 0)
				_dim1 = (input1._dim1*input1._dim2)/_dim2;
		}*/
	}

	@Override
	public Object clone() throws CloneNotSupportedException
	{
		TensorGenOp ret = new TensorGenOp();
		
		//copy generic attributes
		ret.clone(this, false);
		
		//copy specific attributes
		ret._maxNumThreads = _maxNumThreads;
		//note: no deep cp of params since read-only 
		
		return ret;
	}
	
	@Override
	public boolean compare( Hop that ) {
		if( !(that instanceof TensorGenOp) )
			return false;

		TensorGenOp that2 = (TensorGenOp)that;
		boolean ret = (_maxNumThreads == that2._maxNumThreads)
				    && (getInput().size()==that.getInput().size());

		//compare all childs (see reshape, sort)
		if( ret ) //sizes matched
			for( int i=0; i<_input.size(); i++ )
				ret &= getInput().get(i) == that2.getInput().get(i);

		return ret;
	}

}
