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

package org.tugraz.sysds.runtime.io;

import org.tugraz.sysds.conf.ConfigurationManager;
import org.tugraz.sysds.conf.CompilerConfig.ConfigType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.matrix.data.OutputInfo;

public class MatrixWriterFactory 
{

	public static MatrixWriter createMatrixWriter( OutputInfo oinfo ) {
		return createMatrixWriter(oinfo, -1, null);
	}

	public static MatrixWriter createMatrixWriter( OutputInfo oinfo, int replication, FileFormatProperties props ) 
	{
		MatrixWriter writer = null;
		
		if( oinfo == OutputInfo.TextCellOutputInfo ) {
			if( ConfigurationManager.getCompilerConfigFlag(ConfigType.PARALLEL_CP_WRITE_TEXTFORMATS) )
				writer = new WriterTextCellParallel();
			else
				writer = new WriterTextCell();
		}
		else if( oinfo == OutputInfo.MatrixMarketOutputInfo ) {
			//note: disabled parallel cp write of matrix market in order to ensure the
			//requirement of writing out a single file
			
			//if( OptimizerUtils.PARALLEL_CP_WRITE_TEXTFORMATS )
			//	writer = new WriterMatrixMarketParallel();
			
			writer = new WriterMatrixMarket();
		}
		else if( oinfo == OutputInfo.CSVOutputInfo ) {
			if( props!=null && !(props instanceof FileFormatPropertiesCSV) )
				throw new DMLRuntimeException("Wrong type of file format properties for CSV writer.");
			if( ConfigurationManager.getCompilerConfigFlag(ConfigType.PARALLEL_CP_WRITE_TEXTFORMATS) )
				writer = new WriterTextCSVParallel((FileFormatPropertiesCSV)props);
			else
				writer = new WriterTextCSV((FileFormatPropertiesCSV)props);
		}
		else if( oinfo == OutputInfo.LIBSVMOutputInfo) {
			if( props!=null && !(props instanceof FileFormatPropertiesLIBSVM) )
				throw new DMLRuntimeException("Wrong type of file format properties for LIBSVM writer.");
			if( ConfigurationManager.getCompilerConfigFlag(ConfigType.PARALLEL_CP_WRITE_TEXTFORMATS) )
				writer = new WriterTextLIBSVMParallel((FileFormatPropertiesLIBSVM)props);
			else
				writer = new WriterTextLIBSVM((FileFormatPropertiesLIBSVM)props);
		}
		else if( oinfo == OutputInfo.BinaryCellOutputInfo ) {
			writer = new WriterBinaryCell();
		}
		else if( oinfo == OutputInfo.BinaryBlockOutputInfo ) {
			if( ConfigurationManager.getCompilerConfigFlag(ConfigType.PARALLEL_CP_WRITE_BINARYFORMATS) )
				writer = new WriterBinaryBlockParallel(replication);
			else
				writer = new WriterBinaryBlock(replication);
		}
		else {
			throw new DMLRuntimeException("Failed to create matrix writer for unknown output info: "
		                                   + OutputInfo.outputInfoToString(oinfo));
		}
		
		return writer;
	}
	
}
