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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.tugraz.sysds.conf.ConfigurationManager;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.data.DenseBlock;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;

public class ReaderTextLIBSVM extends MatrixReader 
{
	private FileFormatPropertiesLIBSVM _props = null;
	
	public ReaderTextLIBSVM(FileFormatPropertiesLIBSVM props) {
		_props = props;
	}

	@Override
	public MatrixBlock readMatrixFromHDFS(String fname, long rlen, long clen, int brlen, int bclen, long estnnz) 
		throws IOException, DMLRuntimeException 
	{
		//allocate output matrix block
		MatrixBlock ret = null;
		if( rlen>=0 && clen>=0 ) //otherwise allocated on read
			ret = createOutputMatrixBlock(rlen, clen, (int)rlen, (int)clen, estnnz, true, false);
		
		//prepare file access
		JobConf    job  =  new JobConf(ConfigurationManager.getCachedJobConf());	
		Path       path =  new Path( fname );
		FileSystem fs   =  IOUtilFunctions.getFileSystem(path, job);
		
		//check existence and non-empty file
		checkValidInputFile(fs, path); 
	
		//core read 
		ret = readLIBSVMMatrixFromHDFS(path, job, fs, ret, rlen, clen, brlen, bclen, 
				   _props.hasHeader());
		
		//finally check if change of sparse/dense block representation required
		//(nnz explicitly maintained during read)
		ret.examSparsity();
		
		return ret;
	}
	
	@Override
	public MatrixBlock readMatrixFromInputStream(InputStream is, long rlen, long clen, int brlen, int bclen, long estnnz) 
		throws IOException, DMLRuntimeException 
	{
		//allocate output matrix block
		MatrixBlock ret = createOutputMatrixBlock(rlen, clen, (int)rlen, (int)clen, estnnz, true, false);
		
		//core read 
		long lnnz = readLIBSVMMatrixFromInputStream(is, "external inputstream", ret, new MutableInt(0), rlen, clen, 
			brlen, bclen, _props.hasHeader(), true);
				
		//finally check if change of sparse/dense block representation required
		ret.setNonZeros( lnnz );
		ret.examSparsity();
		
		return ret;
	}
	
	@SuppressWarnings("unchecked")
	private static MatrixBlock readLIBSVMMatrixFromHDFS( Path path, JobConf job, FileSystem fs, MatrixBlock dest, 
			long rlen, long clen, int brlen, int bclen, boolean hasHeader)
		throws IOException, DMLRuntimeException
	{
		//prepare file paths in alphanumeric order
		ArrayList<Path> files=new ArrayList<>();
		if(fs.isDirectory(path)) {
			for(FileStatus stat: fs.listStatus(path, IOUtilFunctions.hiddenFileFilter))
				files.add(stat.getPath());
			Collections.sort(files);
		}
		else
			files.add(path);
		
		//determine matrix size via additional pass if required
		if ( dest == null ) {
			dest = computeLIBSVMSize(files, job, fs, hasHeader);
			clen = dest.getNumColumns();
		}
		
		//actual read of individual files
		long lnnz = 0;
		MutableInt row = new MutableInt(0);
		for(int fileNo=0; fileNo<files.size(); fileNo++) {
			lnnz += readLIBSVMMatrixFromInputStream(fs.open(files.get(fileNo)), path.toString(), dest, 
				row, rlen, clen, brlen, bclen, hasHeader, fileNo==0);
		}
		
		//post processing
		dest.setNonZeros( lnnz );
		
		return dest;
	}
	
	private static long readLIBSVMMatrixFromInputStream( InputStream is, String srcInfo, MatrixBlock dest, MutableInt rowPos, 
			long rlen, long clen, int brlen, int bclen, boolean hasHeader, boolean first )
		throws IOException
	{
		boolean sparse    = dest.isInSparseFormat();
		String  value     = null;
		int     row       = rowPos.intValue();
		double  cellValue = 0;
		long    lnnz      = 0;
		
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		if(first && hasHeader ) 
			br.readLine(); //ignore header
		
		// Read the data
		try
		{
			DenseBlock a = (sparse ? null : dest.getDenseBlock());

			while( (value=br.readLine())!=null )  //for each line
			{
				String cellStr = value.toString().trim();
				int    partno = 1;
				int    index = IOUtilFunctions.getIndexLIBSVM(cellStr, partno);
				double indexval = IOUtilFunctions.getIndexValLIBSVM(cellStr, partno);
				int    NumIndices = IOUtilFunctions.getNumIndicesLIBSVM(cellStr);
				int    col = 0;
				double label = 0;
				for ( int i=0; i<clen; i++) 
				{
					if (i == 0) {
						label = IOUtilFunctions.getClassLabelLIBSVM(cellStr); // read the class label
						//fill the last column with class label
						if (sparse) 
							dest.appendValue(row, (int)clen-1, label);
						else
							a.set(row, (int)clen-1, label);
						if (label != 0)
							lnnz++;
						continue;
					}
					
					if (col == index-1) 
					{
						cellValue = indexval;
						partno++;
						index = (partno <= NumIndices) ? IOUtilFunctions.getIndexLIBSVM(cellStr, partno) : 0;
						indexval = (partno <= NumIndices) ? IOUtilFunctions.getIndexValLIBSVM(cellStr, partno) : 0;
					}
					else 
						cellValue = 0;
					
					if ( cellValue != 0 ) {
						if (sparse)
							dest.appendValue(row, col, cellValue);
						else
							a.set(row, col, cellValue);
						lnnz++;
					}
					col++;
				}

				row++;
			}
		}
		finally {
			IOUtilFunctions.closeSilently(br);
		}
		
		rowPos.setValue(row);
		return lnnz;
	}

	private static MatrixBlock computeLIBSVMSize( List<Path> files, JobConf job, FileSystem fs, boolean hasHeader) 
		throws IOException, DMLRuntimeException 
	{		
		int nrow = -1;
		int ncol = -1;
		String value = null;
		
		String cellStr = null;
		for(int fileNo=0; fileNo<files.size(); fileNo++)
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(files.get(fileNo))));	
			try
			{
				// Read the header line, if there is one.
				if(fileNo==0)
				{
					if ( hasHeader ) 
						br.readLine(); //ignore header
					if( (value = br.readLine()) != null ) {
						cellStr = value.toString().trim();
						ncol = IOUtilFunctions.getNumIndicesLIBSVM(cellStr);
						ncol += 1;  // last column for class label
						nrow = 1;
					}
				}
				
				while ( (value = br.readLine()) != null ) {
					nrow++;
				}
			}
			finally {
				IOUtilFunctions.closeSilently(br);
			}
		}
		
		// allocate target matrix block based on given size; 
		return createOutputMatrixBlock(nrow, ncol, 
			nrow, ncol, (long)nrow*ncol, true, false);
	}
}