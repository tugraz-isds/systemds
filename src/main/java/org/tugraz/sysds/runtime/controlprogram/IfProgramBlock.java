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

import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.parser.IfStatementBlock;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.DMLScriptException;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.instructions.Instruction;
import org.tugraz.sysds.runtime.instructions.cp.BooleanObject;


public class IfProgramBlock extends ProgramBlock 
{
	private ArrayList<Instruction> _predicate;
	private ArrayList <Instruction> _exitInstructions ;
	private ArrayList<ProgramBlock> _childBlocksIfBody;
	private ArrayList<ProgramBlock> _childBlocksElseBody;
	
	public IfProgramBlock(Program prog, ArrayList<Instruction> predicate) {
		super(prog);
		_childBlocksIfBody = new ArrayList<>();
		_childBlocksElseBody = new ArrayList<>();
		_predicate = predicate;
		_exitInstructions = new ArrayList<>();
	}
	
	public ArrayList<ProgramBlock> getChildBlocksIfBody() { 
		return _childBlocksIfBody; 
	}

	public void setChildBlocksIfBody(ArrayList<ProgramBlock> blocks) { 
		_childBlocksIfBody = blocks; 
	}
	
	public void addProgramBlockIfBody(ProgramBlock pb) { 
		_childBlocksIfBody.add(pb); 
	}	
	
	public ArrayList<ProgramBlock> getChildBlocksElseBody() { 
		return _childBlocksElseBody; 
	}

	public void setChildBlocksElseBody(ArrayList<ProgramBlock> blocks) { 
		_childBlocksElseBody = blocks; 
	}
	
	public void addProgramBlockElseBody(ProgramBlock pb) {
		_childBlocksElseBody.add(pb); 
	}
	
	public void setExitInstructions2(ArrayList<Instruction> exitInstructions){
		_exitInstructions = exitInstructions;
	}

	public void setExitInstructions1(ArrayList<Instruction> predicate){
		_predicate = predicate;
	}
	
	public void addExitInstruction(Instruction inst){
		_exitInstructions.add(inst);
	}
	
	public ArrayList<Instruction> getPredicate(){
		return _predicate;
	}

	public void setPredicate(ArrayList<Instruction> predicate) {
		_predicate = predicate;
	}
	
	public ArrayList<Instruction> getExitInstructions(){
		return _exitInstructions;
	}
	
	@Override
	public void execute(ExecutionContext ec) 
	{
		BooleanObject predResult = executePredicate(ec);
	
		if (DMLScript.LINEAGE_DEDUP)
			ec.getLineagePath().setBranchPredicateValue(predResult.getBooleanValue());
			
		//execute if statement
		if(predResult.getBooleanValue())
		{	
			try 
			{	
				for (int i=0 ; i < _childBlocksIfBody.size() ; i++) {
					_childBlocksIfBody.get(i).execute(ec);
				}
			}
			catch(DMLScriptException e) {
				throw e;
			}
			catch(Exception e)
			{
				throw new DMLRuntimeException(this.printBlockErrorLocation() + "Error evaluating if statement body ", e);
			}
		}
		else
		{
			try 
			{	
				for (int i=0 ; i < _childBlocksElseBody.size() ; i++) {
					_childBlocksElseBody.get(i).execute(ec);
				}
			}
			catch(DMLScriptException e) {
				throw e;
			}
			catch(Exception e)
			{
				throw new DMLRuntimeException(this.printBlockErrorLocation() + "Error evaluating else statement body ", e);
			}	
		}
		
		//execute exit instructions
		try { 
			executeInstructions(_exitInstructions, ec);
		}
		catch(DMLScriptException e) {
			throw e;
		}
		catch (Exception e){
			
			throw new DMLRuntimeException(this.printBlockErrorLocation() + "Error evaluating if exit instructions ", e);
		}
	}

	private BooleanObject executePredicate(ExecutionContext ec) 
	{
		BooleanObject result = null;
		try
		{
			if( _sb != null )
			{
				IfStatementBlock isb = (IfStatementBlock)_sb;
				result = (BooleanObject) executePredicate(_predicate, isb.getPredicateHops(), 
					isb.requiresPredicateRecompilation(), ValueType.BOOLEAN, ec);
			}
			else
				result = (BooleanObject) executePredicate(_predicate, null, false, ValueType.BOOLEAN, ec);
		}
		catch(Exception ex) {
			throw new DMLRuntimeException(this.printBlockErrorLocation() + "Failed to evaluate the IF predicate.", ex);
		}
		
		//(guaranteed to be non-null, see executePredicate/getScalarInput)
		return result;
	}
	
	@Override
	public String printBlockErrorLocation(){
		return "ERROR: Runtime error in if program block generated from if statement block between lines " + _beginLine + " and " + _endLine + " -- ";
	}
}
