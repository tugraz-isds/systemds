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

package org.tugraz.sysds.runtime.instructions.cp;

import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.lops.LeftIndex;
import org.tugraz.sysds.lops.RightIndex;
import org.tugraz.sysds.common.Types.DataType;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.caching.MatrixObject;
import org.tugraz.sysds.runtime.controlprogram.caching.MatrixObject.UpdateType;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.lineage.Lineage;
import org.tugraz.sysds.runtime.lineage.LineageItem;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.IndexRange;
import org.tugraz.sysds.utils.Statistics;

import java.util.ArrayList;

public final class MatrixIndexingCPInstruction extends IndexingCPInstruction {

	public MatrixIndexingCPInstruction(CPOperand in, CPOperand rl, CPOperand ru, CPOperand cl, CPOperand cu,
			CPOperand out, String opcode, String istr) {
		super(in, rl, ru, cl, cu, out, opcode, istr);
	}

	protected MatrixIndexingCPInstruction(CPOperand lhsInput, CPOperand rhsInput, CPOperand rl,
			CPOperand ru, CPOperand cl, CPOperand cu, CPOperand out, String opcode, String istr) {
		super(lhsInput, rhsInput, rl, ru, cl, cu, out, opcode, istr);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		String opcode = getOpcode();
		IndexRange ixrange = getIndexRange(ec);
		
		//get original matrix
		MatrixObject mo = ec.getMatrixObject(input1.getName());
		
		//right indexing
		if( opcode.equalsIgnoreCase(RightIndex.OPCODE) )
		{
			MatrixBlock resultBlock = null;
			
			if( mo.isPartitioned() ) //via data partitioning
				resultBlock = mo.readMatrixPartition(ixrange.add(1));
			else //via slicing the in-memory matrix
			{
				//execute right indexing operation (with shallow row copies for range
				//of entire sparse rows, which is safe due to copy on update)
				MatrixBlock matBlock = ec.getMatrixInput(input1.getName());
				resultBlock = matBlock.slice((int)ixrange.rowStart, (int)ixrange.rowEnd, 
					(int)ixrange.colStart, (int)ixrange.colEnd, false, new MatrixBlock());
				
				//unpin rhs input
				ec.releaseMatrixInput(input1.getName());
				
				//ensure correct sparse/dense output representation
				if( checkGuardedRepresentationChange(matBlock, resultBlock) )
					resultBlock.examSparsity();
			}
			
			//unpin output
			ec.setMatrixOutput(output.getName(), resultBlock);
		}
		//left indexing
		else if ( opcode.equalsIgnoreCase(LeftIndex.OPCODE))
		{
			UpdateType updateType = mo.getUpdateType();
			if(DMLScript.STATISTICS) {
				if( updateType.isInPlace() )
					Statistics.incrementTotalLixUIP();
				Statistics.incrementTotalLix();
			}
			
			MatrixBlock matBlock = ec.getMatrixInput(input1.getName());
			MatrixBlock resultBlock = null;
			
			if(input2.getDataType() == DataType.MATRIX) { //MATRIX<-MATRIX
				MatrixBlock rhsMatBlock = ec.getMatrixInput(input2.getName());
				resultBlock = matBlock.leftIndexingOperations(rhsMatBlock, ixrange, new MatrixBlock(), updateType);
				ec.releaseMatrixInput(input2.getName());
			}
			else { //MATRIX<-SCALAR 
				if(!ixrange.isScalar())
					throw new DMLRuntimeException("Invalid index range of scalar leftindexing: "+ixrange.toString()+"." );
				ScalarObject scalar = ec.getScalarInput(input2.getName(), ValueType.FP64, input2.isLiteral());
				resultBlock = (MatrixBlock) matBlock.leftIndexingOperations(scalar, 
					(int)ixrange.rowStart, (int)ixrange.colStart, new MatrixBlock(), updateType);
			}

			//unpin lhs input
			ec.releaseMatrixInput(input1.getName());
			
			//ensure correct sparse/dense output representation
			//(memory guarded by release of input)
			resultBlock.examSparsity();
			
			//unpin output
			ec.setMatrixOutput(output.getName(), resultBlock, updateType, getExtendedOpcode());
		}
		else
			throw new DMLRuntimeException("Invalid opcode (" + opcode +") encountered in MatrixIndexingCPInstruction.");
	}
	
	@Override
	public LineageItem[] getLineageItems() {
		ArrayList<LineageItem> lineages = new ArrayList<>();
		if (input1 != null)
			lineages.add(Lineage.getOrCreate(input1));
		if (input2 != null)
			lineages.add(Lineage.getOrCreate(input2));
		if (input3 != null)
			lineages.add(Lineage.getOrCreate(input3));
		if (colLower != null)
			lineages.add(Lineage.getOrCreate(colLower));
		if (colUpper != null)
			lineages.add(Lineage.getOrCreate(colUpper));
		if (rowLower != null)
			lineages.add(Lineage.getOrCreate(rowLower));
		if (rowUpper != null)
			lineages.add(Lineage.getOrCreate(rowUpper));
		
		return new LineageItem[]{new LineageItem(output.getName(),
				getOpcode(), lineages.toArray(new LineageItem[0]))};
	}
}
