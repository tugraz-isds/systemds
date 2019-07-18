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

package org.tugraz.sysds.lops;

import org.tugraz.sysds.common.Types.DataType;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.hops.AggBinaryOp.SparkAggType;
import org.tugraz.sysds.lops.LopProperties.ExecType;

public class TensorPartialAggregate extends Lop
{
	public enum DirectionTypes {
		RowCol,
		Row,
		Col
	}

	private Aggregate.OperationTypes operation;
	private DirectionTypes direction;

	//optional attribute for CP num threads
	private int _numThreads = -1;

	//optional attribute for spark exec type
	private SparkAggType _aggtype = SparkAggType.MULTI_BLOCK;

	public TensorPartialAggregate(Lop input, Aggregate.OperationTypes op,
	                              TensorPartialAggregate.DirectionTypes direct, DataType dt, ValueType vt, ExecType et, int k)
	{
		super(Type.PartialAggregate, dt, vt);
		init(input, op, direct, dt, vt, et);
		_numThreads = k;
	}

	public TensorPartialAggregate(Lop input, Aggregate.OperationTypes op,
	                              TensorPartialAggregate.DirectionTypes direct, DataType dt, ValueType vt, SparkAggType aggtype, ExecType et)
	{
		super(Type.PartialAggregate, dt, vt);
		init(input, op, direct, dt, vt, et);
		_aggtype = aggtype;
	}

	/**
	 * Constructor to setup a partial aggregate operation.
	 *
	 * @param input low-level operator
	 * @param op aggregate operation type
	 * @param direct partial aggregate directon type
	 * @param dt data type
	 * @param vt value type
	 * @param et execution type
	 */
	private void init(Lop input,
	                  Aggregate.OperationTypes op,
	                  TensorPartialAggregate.DirectionTypes direct, DataType dt, ValueType vt, ExecType et) {
		operation = op;
		direction = direct;
		this.addInput(input);
		input.addOutput(this);
		lps.setProperties(inputs, et);
	}

	@Override
	public String toString() {
		return "Partial Aggregate " + operation;
	}
	
	private String getOpcode() {
		return getOpcode(operation, direction);
	}

	/**
	 * Instruction generation for for CP and Spark
	 */
	@Override
	public String getInstructions(String input1, String output) 
	{
		StringBuilder sb = new StringBuilder();
		sb.append( getExecType() );
		
		sb.append( OPERAND_DELIMITOR );
		sb.append( getOpcode() );
		
		sb.append( OPERAND_DELIMITOR );
		sb.append( getInputs().get(0).prepInputOperand(input1) );
		
		sb.append( OPERAND_DELIMITOR );
		sb.append( prepOutputOperand(output) );
		
		//exec-type specific attributes
		sb.append( OPERAND_DELIMITOR );
		if( getExecType() == ExecType.SPARK )
			// TODO support SPARK
			sb.append( _aggtype );
		else if( getExecType() == ExecType.CP )
			sb.append( _numThreads );
		
		return sb.toString();
	}

	public static String getOpcode(Aggregate.OperationTypes op, DirectionTypes dir) 
	{
		switch( op )
		{
			case Sum: {
				if( dir == DirectionTypes.RowCol ) 
					return "tua+";
				else if( dir == DirectionTypes.Row ) 
					return "tuar+";
				else if( dir == DirectionTypes.Col ) 
					return "tuac+";
				break;
			}
			// TODO other Aggregations
		}
		
		//should never come here for normal compilation
		throw new UnsupportedOperationException("Instruction is not defined for PartialAggregate operation " + op);
	}

}
