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

package org.tugraz.sysds.lops;

import java.util.HashMap;

import org.tugraz.sysds.hops.Hop.DataGenMethod;
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.lops.OutputParameters.Format;
import org.tugraz.sysds.parser.DataExpression;
import org.tugraz.sysds.parser.DataIdentifier;
import org.tugraz.sysds.parser.Statement;
import org.tugraz.sysds.common.Types.DataType;
import org.tugraz.sysds.common.Types.ValueType;


/**
 * <p>Defines a LOP that generates data.</p>
 */
public class DataGen extends Lop
{
	public static final String RAND_OPCODE   = "rand"; //rand
	public static final String SEQ_OPCODE    = "seq"; //sequence
	public static final String SINIT_OPCODE  = "sinit"; //string initialize
	public static final String SAMPLE_OPCODE = "sample"; //sample.int
	public static final String TIME_OPCODE = "time"; //time
	
	private int _numThreads = 1;
	
	/** base dir for rand input */
	private String baseDir;
	
	private HashMap<String, Lop> _inputParams;
	DataGenMethod method;
	
	/**
	 * <p>Creates a new Rand-LOP. The target identifier has to hold the dimensions of the new random object.</p>
	 * 
	 * @param mthd data gen method
	 * @param id target identifier
	 * @param inputParametersLops Lops of input parameters
	 * @param baseDir base dir for runtime
	 * @param dt Data type
	 * @param vt Value type
	 * @param et Execution type
	 */
	public DataGen(DataGenMethod mthd, DataIdentifier id, HashMap<String, Lop> 
				inputParametersLops, String baseDir, DataType dt, ValueType vt, ExecType et)
	{
		super(Type.DataGen, dt, vt);
		method = mthd;
				
		for (Lop lop : inputParametersLops.values()) {
			this.addInput(lop);
			lop.addOutput(this);
		}
		
		_inputParams = inputParametersLops;
		
		init(id, baseDir, et);
	}

	public DataGenMethod getDataGenMethod() {
		return method;
	}
	
	public void init(DataIdentifier id, String baseDir, ExecType et)
	{
		this.getOutputParameters().setFormat(Format.BINARY);
		this.getOutputParameters().setBlocked(true);
		this.getOutputParameters().setNumRows(id.getDim1());
		this.getOutputParameters().setNumCols(id.getDim2());
		this.getOutputParameters().setNnz(-1);
		this.getOutputParameters().setRowsInBlock(id.getRowsInBlock());
		this.getOutputParameters().setColsInBlock(id.getColumnsInBlock());
		lps.setProperties( inputs, et);
	}

	/**
	 * Function to generate CP/SP instructions for data generation via Rand and Seq.
	 * Since DataGen Lop can have many inputs, ONLY the output variable name is 
	 * passed from piggybacking as the function argument <code>output</code>. 
	 */
	@Override
	public String getInstructions(String output)
	{
		switch( method ) 
		{
			case RAND:
				return getRandInstructionCPSpark(output);
			case SINIT:
				return getSInitInstructionCPSpark(output);
			case SEQ:
				return getSeqInstructionCPSpark(output);
			case SAMPLE:
				return getSampleInstructionCPSpark(output);
			case TIME:
				return getTimeInstructionCP(output);
				
			default:
				throw new LopsException("Unknown data generation method: " + method);
		}
	}
	
	/**
	 * Private method that generates CP Instruction for Rand.
	 * 
	 * @param output output operand
	 * @return cp instruction for rand
	 */
	private String getRandInstructionCPSpark(String output) 
	{
		//sanity checks
		if ( method != DataGenMethod.RAND )
			throw new LopsException("Invalid instruction generation for data generation method " + method);
		if( getInputs().size() != DataExpression.RAND_VALID_PARAM_NAMES.length ) {
			throw new LopsException(printErrorLocation() + "Invalid number of operands (" 
				+ getInputs().size() + ") for a Rand operation");
		}
		
		StringBuilder sb = new StringBuilder( );
		
		sb.append( getExecType() );
		sb.append( Lop.OPERAND_DELIMITOR );

		sb.append(RAND_OPCODE);
		sb.append(OPERAND_DELIMITOR);
		
		Lop iLop = _inputParams.get(DataExpression.RAND_ROWS.toString());
		sb.append(iLop.prepScalarInputOperand(getExecType()));
		sb.append(OPERAND_DELIMITOR);

		iLop = _inputParams.get(DataExpression.RAND_COLS.toString());
		sb.append(iLop.prepScalarInputOperand(getExecType()));
		sb.append(OPERAND_DELIMITOR);

		sb.append(String.valueOf(getOutputParameters().getRowsInBlock()));
		sb.append(OPERAND_DELIMITOR);

		sb.append(String.valueOf(getOutputParameters().getColsInBlock()));
		sb.append(OPERAND_DELIMITOR);

		iLop = _inputParams.get(DataExpression.RAND_MIN.toString());
		sb.append(iLop.prepScalarLabel());
		sb.append(OPERAND_DELIMITOR);
		
		iLop = _inputParams.get(DataExpression.RAND_MAX.toString());
		sb.append(iLop.prepScalarLabel());
		sb.append(OPERAND_DELIMITOR);
		
		iLop = _inputParams.get(DataExpression.RAND_SPARSITY.toString());
		if (iLop.isVariable())
			sb.append(iLop.prepScalarLabel());
		else
			sb.append(iLop.getOutputParameters().getLabel()); 
		sb.append(OPERAND_DELIMITOR);
		
		iLop = _inputParams.get(DataExpression.RAND_SEED.toString());
		sb.append(iLop.prepScalarLabel());
		sb.append(OPERAND_DELIMITOR);
		
		if( getExecType() == ExecType.SPARK ) {
			sb.append(baseDir);
			sb.append(OPERAND_DELIMITOR);
		}
		
		iLop = _inputParams.get(DataExpression.RAND_PDF.toString()); //no variable support
		if (iLop.isVariable())
			throw new LopsException(printErrorLocation()
					+ "Parameter " + DataExpression.RAND_PDF
					+ " must be a literal for a Rand operation.");
		sb.append(iLop.getOutputParameters().getLabel()); 
		sb.append(OPERAND_DELIMITOR);
		
		iLop = _inputParams.get(DataExpression.RAND_LAMBDA.toString()); //no variable support
		sb.append(iLop == null ? "" : iLop.prepScalarLabel()); 
		sb.append(OPERAND_DELIMITOR);
		
		if( getExecType() == ExecType.CP ) {
			//append degree of parallelism
			sb.append( _numThreads );
			sb.append( OPERAND_DELIMITOR );
		}
		
		sb.append( prepOutputOperand(output));

		return sb.toString(); 
	}

	private String getSInitInstructionCPSpark(String output) 
	{
		if ( method != DataGenMethod.SINIT )
			throw new LopsException("Invalid instruction generation for data generation method " + method);
		
		//prepare instruction parameters
		Lop iLop = _inputParams.get(DataExpression.RAND_ROWS.toString());
		String rowsString = iLop.prepScalarLabel();
		
		iLop = _inputParams.get(DataExpression.RAND_COLS.toString());
		String colsString = iLop.prepScalarLabel();
		
		String rowsInBlockString = String.valueOf(this
				.getOutputParameters().getRowsInBlock());
		String colsInBlockString = String.valueOf(this
				.getOutputParameters().getColsInBlock());

		iLop = _inputParams.get(DataExpression.RAND_MIN.toString());
		String minString = iLop.getOutputParameters().getLabel();
		if (iLop.isVariable())
			throw new LopsException(this.printErrorLocation()
					+ "Parameter " + DataExpression.RAND_MIN
					+ " must be a literal for a Rand operation.");

		//generate instruction		
		StringBuilder sb = new StringBuilder( );
		ExecType et = getExecType();
		
		sb.append( et );
		sb.append( Lop.OPERAND_DELIMITOR );
		sb.append(SINIT_OPCODE);
		sb.append(OPERAND_DELIMITOR);
		sb.append(rowsString);
		sb.append(OPERAND_DELIMITOR);
		sb.append(colsString);
		sb.append(OPERAND_DELIMITOR);
		sb.append(rowsInBlockString);
		sb.append(OPERAND_DELIMITOR);
		sb.append(colsInBlockString);
		sb.append(OPERAND_DELIMITOR);
		sb.append(minString);
		sb.append(OPERAND_DELIMITOR);
		sb.append(prepOutputOperand(output));

		return sb.toString();
	}
	
	private String getSampleInstructionCPSpark(String output) 
	{
		if ( method != DataGenMethod.SAMPLE )
			throw new LopsException("Invalid instruction generation for data generation method " + method);
		
		//prepare instruction parameters
		Lop lsize = _inputParams.get(DataExpression.RAND_ROWS.toString());
		String size = lsize.prepScalarInputOperand(getExecType());
		
		Lop lrange = _inputParams.get(DataExpression.RAND_MAX.toString());
		String range = lrange.prepScalarLabel();
		
		Lop lreplace = _inputParams.get(DataExpression.RAND_PDF.toString());
		String replace = lreplace.prepScalarLabel();
		
		Lop lseed = _inputParams.get(DataExpression.RAND_SEED.toString());
		String seed = lseed.prepScalarLabel();
		
		String rowsInBlockString = String.valueOf(this.getOutputParameters().getRowsInBlock());
		String colsInBlockString = String.valueOf(this.getOutputParameters().getColsInBlock());
		
		StringBuilder sb = new StringBuilder();
		sb.append( getExecType() );
		sb.append( Lop.OPERAND_DELIMITOR );
		sb.append( "sample" );
		sb.append( Lop.OPERAND_DELIMITOR );
		sb.append( range );
		sb.append( Lop.OPERAND_DELIMITOR );
		sb.append( size );
		sb.append( Lop.OPERAND_DELIMITOR );
		sb.append( replace );
		sb.append( Lop.OPERAND_DELIMITOR );
		sb.append( seed );
		sb.append( Lop.OPERAND_DELIMITOR );
		sb.append( rowsInBlockString );
		sb.append( Lop.OPERAND_DELIMITOR );
		sb.append( colsInBlockString );
		sb.append( Lop.OPERAND_DELIMITOR );
		sb.append( prepOutputOperand(output) );
		
		return sb.toString();
	}
	
	private String getTimeInstructionCP(String output)
	{
		if (method != DataGenMethod.TIME )
			throw new LopsException("Invalid instruction generation for data generation method " + method);
		StringBuilder sb = new StringBuilder();
		sb.append( getExecType() );
		sb.append( Lop.OPERAND_DELIMITOR );
		sb.append( "time" );
		sb.append( Lop.OPERAND_DELIMITOR );
		sb.append( prepOutputOperand(output) );
		
		return sb.toString();
	}
	
	/**
	 * Private method that generates CP Instruction for Seq.
	 * 
	 * @param output output operand
	 * @return cp instruction for seq
	 */
	private String getSeqInstructionCPSpark(String output) {
		if ( method != DataGenMethod.SEQ )
			throw new LopsException("Invalid instruction generation for data generation method " + method);
		
		StringBuilder sb = new StringBuilder( );
		ExecType et = getExecType();
		sb.append( et );
		sb.append( Lop.OPERAND_DELIMITOR );

		Lop iLop = null;

		iLop = _inputParams.get(Statement.SEQ_FROM.toString()); 
		String fromString = iLop.prepScalarInputOperand(et);
		
		iLop = _inputParams.get(Statement.SEQ_TO.toString());
		String toString = iLop.prepScalarInputOperand(et);
		
		iLop = _inputParams.get(Statement.SEQ_INCR.toString()); 
		String incrString = iLop.prepScalarInputOperand(et);
		
		String rowsString = String.valueOf(this.getOutputParameters().getNumRows());
		String colsString = String.valueOf(this.getOutputParameters().getNumCols());
		String rowsInBlockString = String.valueOf(this.getOutputParameters().getRowsInBlock());
		String colsInBlockString = String.valueOf(this.getOutputParameters().getColsInBlock());
		
		sb.append( DataGen.SEQ_OPCODE );
		sb.append( OPERAND_DELIMITOR );
		sb.append( rowsString );
		sb.append( OPERAND_DELIMITOR );
		sb.append( colsString );
		sb.append( OPERAND_DELIMITOR );
		sb.append( rowsInBlockString );
		sb.append( OPERAND_DELIMITOR );
		sb.append( colsInBlockString );
		sb.append( OPERAND_DELIMITOR );
		sb.append( fromString );
		sb.append( OPERAND_DELIMITOR );
		sb.append( toString );
		sb.append( OPERAND_DELIMITOR );
		sb.append( incrString );
		sb.append( OPERAND_DELIMITOR );
		sb.append(prepOutputOperand(output));

		return sb.toString();
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append(method.toString());
		sb.append(" ; num_rows=" + this.getOutputParameters().getNumRows());
		sb.append(" ; num_cols=" + this.getOutputParameters().getNumCols());
		sb.append(" ; nnz=" + this.getOutputParameters().getNnz());
		sb.append(" ; num_rows_per_block=" + this.getOutputParameters().getRowsInBlock());
		sb.append(" ; num_cols_per_block=" + this.getOutputParameters().getColsInBlock());
		sb.append(" ; format=" + this.getOutputParameters().getFormat());
		sb.append(" ; blocked=" + this.getOutputParameters().isBlocked());
		sb.append(" ; dir=" + this.baseDir);
		return sb.toString();
	}
	
	public void setNumThreads(int k) {
		_numThreads = k;
	}
}
