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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.tugraz.sysds.conf.ConfigurationManager;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.data.SparseBlock;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.HDFSTool;

public class WriterTextLIBSVM extends MatrixWriter
{
	//blocksize for string concatenation in order to prevent write OOM 
	//(can be set to very large value to disable blocking)
	public static final int BLOCKSIZE_J = 32; //32 cells (typically ~512B, should be less than write buffer of 1KB)
	
	protected FileFormatPropertiesLIBSVM _props = null;
	
	public WriterTextLIBSVM( FileFormatPropertiesLIBSVM props ) {
		_props = props;
	}

	@Override
	public final void writeMatrixToHDFS(MatrixBlock src, String fname, long rlen, long clen, int brlen, int bclen, long nnz, boolean diag) 
		throws IOException, DMLRuntimeException 
	{
		//validity check matrix dimensions
		if( src.getNumRows() != rlen || src.getNumColumns() != clen )
			throw new IOException("Matrix dimensions mismatch with metadata: "+src.getNumRows()+"x"+src.getNumColumns()+" vs "+rlen+"x"+clen+".");
		if( rlen == 0 || clen == 0 )
			throw new IOException("Write of matrices with zero rows or columns not supported ("+rlen+"x"+clen+").");
				
		//prepare file access
		JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());
		Path path = new Path( fname );
		FileSystem fs = IOUtilFunctions.getFileSystem(path, job);
				
		//if the file already exists on HDFS, remove it.
		HDFSTool.deleteFileIfExistOnHDFS( fname );
					
		//core write (sequential/parallel)
		writeLIBSVMMatrixToHDFS(path, job, fs, src, _props);

		IOUtilFunctions.deleteCrcFilesFromLocalFileSystem(fs, path);
	}

	@Override
	public final void writeEmptyMatrixToHDFS(String fname, long rlen, long clen, int brlen, int bclen) 
		throws IOException, DMLRuntimeException 
	{
		;
	}

	protected void writeLIBSVMMatrixToHDFS(Path path, JobConf job, FileSystem fs, MatrixBlock src, FileFormatPropertiesLIBSVM libsvmprops) 
		throws IOException 
	{
		//sequential write libsvm file
		writeLIBSVMMatrixToFile(path, job, fs, src, 0, (int)src.getNumRows(), libsvmprops);
	}
	
	protected static void writeLIBSVMMatrixToFile( Path path, JobConf job, FileSystem fs, MatrixBlock src, int rl, int rlen, FileFormatPropertiesLIBSVM props )
		throws IOException
	{
		boolean sparse = src.isInSparseFormat();
		int clen = src.getNumColumns();
			
		//create buffered writer
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));	
		
		try
		{
			StringBuilder sb = new StringBuilder();
			props = (props==null)? new FileFormatPropertiesLIBSVM() : props;
			boolean libsvmsparse = props.isSparse();
			
			// Write data lines
			if( sparse ) //SPARSE
			{	
				SparseBlock sblock = src.getSparseBlock();
				for(int i=rl; i < rlen; i++) 
	            {
					//write row chunk-wise to prevent OOM on large number of columns
					int prev_jix = -1;
					if(    sblock!=null && i<sblock.numRows() 
						&& !sblock.isEmpty(i) )
					{
						int pos = sblock.pos(i);
						int alen = sblock.size(i);
						int[] aix = sblock.indexes(i);
						double[] avals = sblock.values(i);
						
						// write the class label in the 1st column
						double label = sblock.get(i, clen-1);
						sb.append(label);
						sb.append(IOUtilFunctions.LIBSVM_DELIM);
						int lim = (label == 0) ? pos+alen : pos+alen-1;
						
						for(int j=pos; j<lim; j++) 
						{
							int jix = aix[j];
							
							// output empty fields, if needed
							for( int j2=prev_jix; j2<jix-1; j2++ ) {
								if( !libsvmsparse)
								{
									sb.append(IOUtilFunctions.setIndexValLIBSVM(0, j2));
									sb.append(IOUtilFunctions.LIBSVM_DELIM);
								}
							
								//flush buffered string
					            if( j2%BLOCKSIZE_J==0 ){
									br.write( sb.toString() );
						            sb.setLength(0);
					            }
							}
							
							// output the value (non-zero)
							sb.append(IOUtilFunctions.setIndexValLIBSVM(avals[j], jix));
							if( jix < clen-2)
								sb.append(IOUtilFunctions.LIBSVM_DELIM);
							br.write( sb.toString() );
				            sb.setLength(0);
				            
				            //flush buffered string
				            if( jix%BLOCKSIZE_J==0 ){
								br.write( sb.toString() );
					            sb.setLength(0);
				            }
				            
							prev_jix = jix;
						}
					}
					
					// Output empty fields at the end of the row.
					for( int bj=prev_jix+1; bj<clen-1; bj+=BLOCKSIZE_J )
					{
						for( int j = bj; j < Math.min(clen-1,bj+BLOCKSIZE_J); j++) {
							if( !libsvmsparse)
								sb.append(IOUtilFunctions.setIndexValLIBSVM(0, j));
							if( j < clen-2 )
								sb.append(IOUtilFunctions.LIBSVM_DELIM);
						}
						br.write( sb.toString() );
			            sb.setLength(0);	
					}

					sb.append('\n');
					br.write( sb.toString() ); 
					sb.setLength(0); 
				}
			}
			else
			{
				for (int i=rl; i<rlen; i++)
				{

					// write the class label in the 1st column
					double label = src.getValueDenseUnsafe(i, clen-1);
					sb.append(label);
					sb.append(IOUtilFunctions.LIBSVM_DELIM);
					//write row chunk-wise to prevent OOM on large number of columns
					for( int bj=0; bj<clen-1; bj+=BLOCKSIZE_J )
					{
						for( int j=bj; j<Math.min(clen-1,bj+BLOCKSIZE_J); j++ )
						{
							double lvalue = src.getValueDenseUnsafe(i, j);
							if( lvalue != 0 ) //for nnz
								sb.append(IOUtilFunctions.setIndexValLIBSVM(lvalue, j));
							else if (!libsvmsparse)
								sb.append(IOUtilFunctions.setIndexValLIBSVM(0, j));
							
							if ((j != clen-2) && !((lvalue == 0) && libsvmsparse))
								sb.append(IOUtilFunctions.LIBSVM_DELIM);
						}

						br.write(sb.toString());
						sb.setLength(0);
					}
					
					sb.append('\n');
					br.write(sb.toString());
					sb.setLength(0);
				}
			}
		}
		finally {
			IOUtilFunctions.closeSilently(br);
		}
	}

	public final void addHeaderToLIBSVM(String srcFileName, String destFileName, long rlen, long clen) 
		throws IOException 
	{
		Configuration conf = new Configuration(ConfigurationManager.getCachedJobConf());

		Path srcFilePath = new Path(srcFileName);
		Path destFilePath = new Path(destFileName);
		FileSystem fs = IOUtilFunctions.getFileSystem(srcFilePath, conf);
		
		if ( !_props.hasHeader() ) {
			// simply move srcFile to destFile
			// delete the destination file, if exists already
			fs.delete(destFilePath, true);
			
			fs.createNewFile(destFilePath);
			
			// delete the file "file.libsvm" but preserve the directory structure 
			fs.delete(destFilePath, true);
			
			// finally, move the data to destFilePath 
			fs.rename(srcFilePath, destFilePath);

			return;
		}
	}
	

}
