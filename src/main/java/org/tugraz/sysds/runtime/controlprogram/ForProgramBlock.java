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

package org.tugraz.sysds.runtime.controlprogram;

import java.util.ArrayList;
import java.util.Iterator;

import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.hops.Hop;
import org.tugraz.sysds.parser.ForStatementBlock;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.DMLScriptException;
import org.tugraz.sysds.runtime.controlprogram.caching.MatrixObject.UpdateType;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.instructions.Instruction;
import org.tugraz.sysds.runtime.instructions.cp.IntObject;
import org.tugraz.sysds.runtime.instructions.cp.ScalarObject;
import org.tugraz.sysds.runtime.lineage.Lineage;

public class ForProgramBlock extends ProgramBlock
{
	protected ArrayList<Instruction> _fromInstructions;
	protected ArrayList<Instruction> _toInstructions;
	protected ArrayList<Instruction> _incrementInstructions;
	protected ArrayList <Instruction> _exitInstructions;
	protected ArrayList<ProgramBlock> _childBlocks;
	protected final String _iterPredVar; 
	
	public ForProgramBlock(Program prog, String iterPredVar) {
		super(prog);
		_exitInstructions = new ArrayList<>();
		_childBlocks = new ArrayList<>();
		_iterPredVar = iterPredVar;
	}
	
	public ArrayList<Instruction> getFromInstructions() {
		return _fromInstructions;
	}
	
	public void setFromInstructions(ArrayList<Instruction> instructions) {
		_fromInstructions = instructions;
	}
	
	public ArrayList<Instruction> getToInstructions() {
		return _toInstructions;
	}
	
	public void setToInstructions(ArrayList<Instruction> instructions) {
		_toInstructions = instructions;
	}
	
	public ArrayList<Instruction> getIncrementInstructions() {
		return _incrementInstructions;
	}
	
	public void setIncrementInstructions(ArrayList<Instruction> instructions) {
		_incrementInstructions = instructions;
	}

	public ArrayList<Instruction> getExitInstructions() {
		return _exitInstructions;
	}
	
	public void setExitInstructions(ArrayList<Instruction> inst) {
		_exitInstructions = inst;
	}
	
	public void addProgramBlock(ProgramBlock childBlock) {
		_childBlocks.add(childBlock);
	}
	
	public ArrayList<ProgramBlock> getChildBlocks() {
		return _childBlocks;
	}
	
	public void setChildBlocks(ArrayList<ProgramBlock> pbs) {
		_childBlocks = pbs;
	}
	
	public String getIterVar() {
		return _iterPredVar;
	}
	
	@Override	
	public void execute(ExecutionContext ec) {
		// evaluate from, to, incr only once (assumption: known at for entry)
		IntObject from = executePredicateInstructions( 1, _fromInstructions, ec );
		IntObject to   = executePredicateInstructions( 2, _toInstructions, ec );
		IntObject incr = (_incrementInstructions == null || _incrementInstructions.isEmpty()) ? 
			new IntObject((from.getLongValue()<=to.getLongValue()) ? 1 : -1) :
			executePredicateInstructions( 3, _incrementInstructions, ec );
		
		if ( incr.getLongValue() == 0 ) //would produce infinite loop
			throw new DMLRuntimeException(printBlockErrorLocation() +  "Expression for increment "
				+ "of variable '" + _iterPredVar + "' must evaluate to a non-zero value.");
		
		// execute for loop
		try 
		{
			// prepare update in-place variables
			UpdateType[] flags = prepareUpdateInPlaceVariables(ec, _tid);
			
			// observe all distinct paths, compute a LineageDedupBlock and stores them globally
			if (DMLScript.LINEAGE_DEDUP)
				Lineage.computeDedupBlock(this, ec);
			
			// run for loop body for each instance of predicate sequence 
			SequenceIterator seqIter = new SequenceIterator(from, to, incr);
			for( IntObject iterVar : seqIter ) 
			{
				if (DMLScript.LINEAGE_DEDUP)
					ec.getLineagePath().clearLastBranch();
				
				//set iteration variable
				ec.setVariable(_iterPredVar, iterVar); 
				
				//execute all child blocks
				for(int i=0 ; i < this._childBlocks.size() ; i++) {
					_childBlocks.get(i).execute(ec);
				}
				
				if (DMLScript.LINEAGE_DEDUP)
					Lineage.tracePath(ec.getLineagePath().getLastBranch());
			}
			
			// clear current LineageDedupBlock
			if (DMLScript.LINEAGE_DEDUP)
				Lineage.clearDedupBlock(ec);
			
			// reset update-in-place variables
			resetUpdateInPlaceVariableFlags(ec, flags);
		}
		catch (DMLScriptException e) {
			//propagate stop call
			throw e;
		}
		catch (Exception e) {
			throw new DMLRuntimeException(printBlockErrorLocation() + "Error evaluating for program block", e);
		}
		
		//execute exit instructions
		try {
			executeInstructions(_exitInstructions, ec);	
		}
		catch (Exception e){
			throw new DMLRuntimeException(printBlockErrorLocation() + "Error evaluating for exit instructions", e);
		}
	}

	protected IntObject executePredicateInstructions( int pos, ArrayList<Instruction> instructions, ExecutionContext ec ) 
	{
		ScalarObject tmp = null;
		IntObject ret = null;
		
		try
		{
			if( _sb != null )
			{
				ForStatementBlock fsb = (ForStatementBlock)_sb;
				Hop predHops = null;
				boolean recompile = false;
				if (pos == 1){ 
					predHops = fsb.getFromHops();
					recompile = fsb.requiresFromRecompilation();
				}
				else if (pos == 2) {
					predHops = fsb.getToHops();
					recompile = fsb.requiresToRecompilation();
				}
				else if (pos == 3){
					predHops = fsb.getIncrementHops();
					recompile = fsb.requiresIncrementRecompilation();
				}
				tmp = (IntObject) executePredicate(instructions, predHops, recompile, ValueType.INT64, ec);
			}
			else
				tmp = (IntObject) executePredicate(instructions, null, false, ValueType.INT64, ec);
		}
		catch(Exception ex)
		{
			String predNameStr = null;
			if 		(pos == 1) predNameStr = "from";
			else if (pos == 2) predNameStr = "to";
			else if (pos == 3) predNameStr = "increment";
			
			throw new DMLRuntimeException(this.printBlockErrorLocation() +"Error evaluating '" + predNameStr + "' predicate", ex);
		}
		
		//final check of resulting int object (guaranteed to be non-null, see executePredicate)
		if( tmp instanceof IntObject )
			ret = (IntObject)tmp;
		else //downcast to int if necessary
			ret = new IntObject(tmp.getLongValue()); 
		
		return ret;
	}
	
	@Override
	public String printBlockErrorLocation(){
		return "ERROR: Runtime error in for program block generated from for statement block between lines " + _beginLine + " and " + _endLine + " -- ";
	}
	
	/**
	 * Utility class for iterating over positive or negative predicate sequences.
	 */
	protected class SequenceIterator implements Iterator<IntObject>, Iterable<IntObject>
	{
		private long _cur = -1;
		private long _to = -1;
		private long _incr = -1;
		private boolean _inuse = false;
		
		protected SequenceIterator(IntObject from, IntObject to, IntObject incr) {
			_cur = from.getLongValue();
			_to = to.getLongValue();
			_incr = incr.getLongValue();
		}

		@Override
		public boolean hasNext() {
			return _incr > 0 ? _cur <= _to : _cur >= _to;
		}

		@Override
		public IntObject next() {
			IntObject ret = new IntObject(_cur);
			_cur += _incr; //update current val
			return ret;
		}

		@Override
		public Iterator<IntObject> iterator() {
			if( _inuse )
				throw new RuntimeException("Unsupported reuse of iterator.");				
			_inuse = true;
			return this;
		}

		@Override
		public void remove() {
			throw new RuntimeException("Unsupported remove on iterator.");
		}
	}
}