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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.tugraz.sysds.conf.ConfigurationManager;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.data.DenseBlock;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.CommonThreadPool;

public class ReaderTextLIBSVMParallel extends MatrixReader
{
	private FileFormatPropertiesLIBSVM _props = null;
	private int _numThreads = 1;

	private SplitOffsetInfos _offsets = null;

	public ReaderTextLIBSVMParallel(FileFormatPropertiesLIBSVM props) {
		_numThreads = OptimizerUtils.getParallelTextReadParallelism();
		_props = props;
	}
	
	@Override
	public MatrixBlock readMatrixFromHDFS(String fname, long rlen, long clen,
			int brlen, int bclen, long estnnz) 
		throws IOException, DMLRuntimeException 
	{
		// prepare file access
		JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());
		Path path = new Path(fname);
		FileSystem fs = IOUtilFunctions.getFileSystem(path, job);
		
		FileInputFormat.addInputPath(job, path);
		TextInputFormat informat = new TextInputFormat();
		informat.configure(job);

		InputSplit[] splits = informat.getSplits(job, _numThreads);
		splits = IOUtilFunctions.sortInputSplits(splits);

		// check existence and non-empty file
		checkValidInputFile(fs, path);

		// allocate output matrix block
		// First Read Pass (count rows/cols, determine offsets, allocate matrix block)
		MatrixBlock ret = computeLIBSVMSizeAndCreateOutputMatrixBlock(splits, path, job, rlen, clen, estnnz);
		rlen = ret.getNumRows();
		clen = ret.getNumColumns();

		// Second Read Pass (read, parse strings, append to matrix block)
		readLIBSVMMatrixFromHDFS(splits, path, job, ret, rlen, clen, brlen, bclen);
		
		//post-processing (representation-specific, change of sparse/dense block representation)
		// - nnz explicitly maintained in parallel for the individual splits
		ret.examSparsity();

		// sanity check for parallel row count (since determined internally)
		if (rlen >= 0 && rlen != ret.getNumRows())
			throw new DMLRuntimeException("Read matrix inconsistent with given meta data: "
					+ "expected nrow="+ rlen + ", real nrow=" + ret.getNumRows());

		return ret;
	}
	
	@Override
	public MatrixBlock readMatrixFromInputStream(InputStream is, long rlen, long clen, int brlen, int bclen, long estnnz) 
		throws IOException, DMLRuntimeException 
	{
		//not implemented yet, fallback to sequential reader
		return new ReaderTextLIBSVM(_props).readMatrixFromInputStream(is, rlen, clen, brlen, bclen, estnnz);
	}
	
	private void readLIBSVMMatrixFromHDFS(InputSplit[] splits, Path path, JobConf job, 
			MatrixBlock dest, long rlen, long clen, int brlen, int bclen) 
		throws IOException 
	{
		FileInputFormat.addInputPath(job, path);
		TextInputFormat informat = new TextInputFormat();
		informat.configure(job);

		ExecutorService pool = CommonThreadPool.get(_numThreads);

		try 
		{
			// create read tasks for all splits
			ArrayList<LIBSVMReadTask> tasks = new ArrayList<>();
			int splitCount = 0;
			for (InputSplit split : splits) {
				tasks.add( new LIBSVMReadTask(split, _offsets, informat, job, dest, rlen, clen, splitCount++) );
			}
			pool.invokeAll(tasks);
			pool.shutdown();

			// check return codes and aggregate nnz
			long lnnz = 0;
			for (LIBSVMReadTask rt : tasks) {
				lnnz += rt.getPartialNnz();
				if (!rt.getReturnCode()) {
					Exception err = rt.getException();
					throw new IOException("Read task for libsvm input failed: "+ err.toString(), err);
				}
			}
			dest.setNonZeros(lnnz);
		} 
		catch (Exception e) {
			throw new IOException("Threadpool issue, while parallel read.", e);
		}
	}
	
	private MatrixBlock computeLIBSVMSizeAndCreateOutputMatrixBlock(InputSplit[] splits, Path path,
			JobConf job, long rlen, long clen, long estnnz)
		throws IOException, DMLRuntimeException 
	{
		int nrow = 0;
		int ncol = 0;
		
		FileInputFormat.addInputPath(job, path);
		TextInputFormat informat = new TextInputFormat();
		informat.configure(job);

		// count no of entities in the first non-header row
		LongWritable key = new LongWritable();
		Text oneLine = new Text();
		RecordReader<LongWritable, Text> reader = informat.getRecordReader(splits[0], job, Reporter.NULL);
		try {
			if (reader.next(key, oneLine)) {
				String cellStr = oneLine.toString().trim();
				ncol = IOUtilFunctions.getNumIndicesLIBSVM(cellStr);
				ncol += 1;  // last column for class label
			}
		} 
		finally {
			IOUtilFunctions.closeSilently(reader);
		}

		// count rows in parallel per split
		try 
		{
			ExecutorService pool = CommonThreadPool.get(_numThreads);
			ArrayList<CountRowsTask> tasks = new ArrayList<>();
			for (InputSplit split : splits) {
				tasks.add(new CountRowsTask(split, informat, job));
			}
			pool.invokeAll(tasks);
			pool.shutdown();

			// collect row counts for offset computation
			// early error notify in case not all tasks successful
			_offsets = new SplitOffsetInfos(tasks.size());
			for (CountRowsTask rt : tasks) {
				if (!rt.getReturnCode())
					throw new IOException("Count task for libsvm input failed: "+ rt.getErrMsg());
				_offsets.setOffsetPerSplit(tasks.indexOf(rt), nrow);
				_offsets.setLenghtPerSplit(tasks.indexOf(rt), rt.getRowCount());
				nrow = nrow + rt.getRowCount();
			}
		} 
		catch (Exception e) {
			throw new IOException("Threadpool Error " + e.getMessage(), e);
		}
		
		//robustness for wrong dimensions which are already compiled into the plan
		if( (rlen != -1 && nrow != rlen) || (clen != -1 && ncol != clen) ) {
			String msg = "Read matrix dimensions differ from meta data: ["+nrow+"x"+ncol+"] vs. ["+rlen+"x"+clen+"].";
			if( rlen < nrow || clen < ncol ) {
				//a) specified matrix dimensions too small
				throw new DMLRuntimeException(msg);
			}
			else {
				//b) specified matrix dimensions too large -> padding and warning
				LOG.warn(msg);
				nrow = (int) rlen;
				ncol = (int) clen;
			}
		}
		
		// allocate target matrix block based on given size; 
		// need to allocate sparse as well since lock-free insert into target
		long estnnz2 = (estnnz < 0) ? (long)nrow * ncol : estnnz;
		return createOutputMatrixBlock(nrow, ncol, nrow, ncol, estnnz2, true, true);
	}
	
	private static class SplitOffsetInfos {
		// offset & length info per split
		private int[] offsetPerSplit = null;
		private int[] lenghtPerSplit = null;

		public SplitOffsetInfos(int numSplits) {
			lenghtPerSplit = new int[numSplits];
			offsetPerSplit = new int[numSplits];
		}

		public int getLenghtPerSplit(int split) {
			return lenghtPerSplit[split];
		}

		public void setLenghtPerSplit(int split, int r) {
			lenghtPerSplit[split] = r;
		}

		public int getOffsetPerSplit(int split) {
			return offsetPerSplit[split];
		}

		public void setOffsetPerSplit(int split, int o) {
			offsetPerSplit[split] = o;
		}
	}
	
	private static class CountRowsTask implements Callable<Object> 
	{
		private InputSplit _split = null;
		private TextInputFormat _informat = null;
		private JobConf _job = null;
		private boolean _rc = true;
		private String _errMsg = null;
		private int _nrows = -1;

		public CountRowsTask(InputSplit split, TextInputFormat informat, JobConf job) {
			_split = split;
			_informat = informat;
			_job = job;
			_nrows = 0;
		}

		public boolean getReturnCode() {
			return _rc;
		}

		public int getRowCount() {
			return _nrows;
		}
		
		public String getErrMsg() {
			return _errMsg;
		}

		@Override
		public Object call() 
			throws Exception 
		{
			RecordReader<LongWritable, Text> reader = _informat.getRecordReader(_split, _job, Reporter.NULL);
			LongWritable key = new LongWritable();
			Text oneLine = new Text();

			try {
				// count rows from the first row
				while (reader.next(key, oneLine)) {
					_nrows++;
				}
			} 
			catch (Exception e) {
				_rc = false;
				_errMsg = "RecordReader error libsvm format. split: "+ _split.toString() + e.getMessage();
				throw new IOException(_errMsg);
			} 
			finally {
				IOUtilFunctions.closeSilently(reader);
			}

			return null;
		}
	}
	
	private static class LIBSVMReadTask implements Callable<Object> 
	{
		private InputSplit _split = null;
		private SplitOffsetInfos _splitoffsets = null;
		private boolean _sparse = false;
		private TextInputFormat _informat = null;
		private JobConf _job = null;
		private MatrixBlock _dest = null;
		private long _rlen = -1;
		private long _clen = -1;
		private int _splitCount = 0;
		
		private boolean _rc = true;
		private Exception _exception = null;
		private long _nnz;
		
		public LIBSVMReadTask(InputSplit split, SplitOffsetInfos offsets,
				TextInputFormat informat, JobConf job, MatrixBlock dest,
				long rlen, long clen, int splitCount) 
		{
			_split = split;
			_splitoffsets = offsets; // new SplitOffsetInfos(offsets);
			_sparse = dest.isInSparseFormat();
			_informat = informat;
			_job = job;
			_dest = dest;
			_rlen = rlen;
			_clen = clen;
			_rc = true;
			_splitCount = splitCount;
		}

		public boolean getReturnCode() {
			return _rc;
		}

		public Exception getException() {
			return _exception;
		}
		
		public long getPartialNnz() {
			return _nnz;
		}
		
		@Override
		public Object call() 
			throws Exception 
		{
			int    row       = 0;
			int    col       = 0;
			double cellValue = 0;
			long   lnnz      = 0;
			
			try 
			{
				RecordReader<LongWritable, Text> reader = _informat.getRecordReader(_split, _job, Reporter.NULL);
				LongWritable key = new LongWritable();
				Text value = new Text();
				
				row = _splitoffsets.getOffsetPerSplit(_splitCount);

				try {
					DenseBlock a = (_sparse ? null :_dest.getDenseBlock());
					while (reader.next(key, value))  // foreach line
					{
						String cellStr = value.toString().trim();
						int    partno = 1;
						int    index = IOUtilFunctions.getIndexLIBSVM(cellStr, partno);
						double indexval = IOUtilFunctions.getIndexValLIBSVM(cellStr, partno);
						int    NumIndices = IOUtilFunctions.getNumIndicesLIBSVM(cellStr);
						double label = 0;
						col          = 0;
						for ( int i=0; i<_clen; i++) 
						{
							if (i == 0) {
								label = IOUtilFunctions.getClassLabelLIBSVM(cellStr); // read the class label
								//fill the last column with class label for dense block
								if (_sparse) 
									_dest.appendValue(row, (int)_clen-1, label);
								else
									a.set(row, (int)_clen-1, label);
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
								if (_sparse)
									_dest.appendValue(row, col, cellValue);
								else
									a.set(row, col, cellValue);
								lnnz++;
							}
							col++;
						}

						row++;
					}

					// sanity checks (number of rows)
					if (row != (_splitoffsets.getOffsetPerSplit(_splitCount) + _splitoffsets.getLenghtPerSplit(_splitCount)) ) 
					{
						throw new IOException("Incorrect number of rows ("+ row+ ") found in delimited file ("
										+ (_splitoffsets.getOffsetPerSplit(_splitCount) 
										+ _splitoffsets.getLenghtPerSplit(_splitCount))+ "): " + value);
					}
				} 
				finally {
					IOUtilFunctions.closeSilently(reader);
				}
			} 
			catch (Exception ex) {
				// central error handling (return code, message)
				_rc = false;
				_exception = ex;

				// post-mortem error handling and bounds checking
				if (row < 0 || row + 1 > _rlen || col < 0 || col + 1 > _clen) {
					String errMsg = "LIBSVM cell [" + (row + 1) + "," + (col + 1)+ "] " + 
							"out of overall matrix range [1:" + _rlen+ ",1:" + _clen + "]. " + ex.getMessage();
					throw new IOException(errMsg, _exception);
				} 
				else {
					String errMsg = "Unable to read matrix in text libsvm format. "+ ex.getMessage();
					throw new IOException(errMsg, _exception);
				}
			}

			//post processing
			_nnz = lnnz;
			
			return null;
		}
	}

}
