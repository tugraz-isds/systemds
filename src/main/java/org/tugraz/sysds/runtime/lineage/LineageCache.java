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

package org.tugraz.sysds.runtime.lineage;

import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.hops.cost.CostEstimatorStaticRuntime;
import org.tugraz.sysds.lops.MMTSJ.MMTSJType;
import org.tugraz.sysds.parser.Statement;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.caching.MatrixObject;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import org.tugraz.sysds.runtime.instructions.CPInstructionParser;
import org.tugraz.sysds.runtime.instructions.Instruction;
import org.tugraz.sysds.runtime.instructions.cp.BinaryMatrixMatrixCPInstruction;
import org.tugraz.sysds.runtime.instructions.cp.CPInstruction.CPType;
import org.tugraz.sysds.runtime.instructions.cp.ComputationCPInstruction;
import org.tugraz.sysds.runtime.instructions.cp.Data;
import org.tugraz.sysds.runtime.instructions.cp.MMTSJCPInstruction;
import org.tugraz.sysds.runtime.instructions.cp.ParameterizedBuiltinCPInstruction;
import org.tugraz.sysds.runtime.instructions.cp.ScalarObject;
import org.tugraz.sysds.runtime.lineage.LineageCacheConfig.ReuseCacheType;
import org.tugraz.sysds.runtime.matrix.data.InputInfo;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.data.OutputInfo;
import org.tugraz.sysds.runtime.meta.MetaDataFormat;
import org.tugraz.sysds.runtime.util.LocalFileUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class LineageCache {
	private static final Map<LineageItem, Entry> _cache = new HashMap<>();
	private static final Map<LineageItem, SpilledItem> _spillList = new HashMap<>();
	private static final HashSet<LineageItem> _removelist = new HashSet<>();
	private static final double CACHE_FRAC = 0.05; // 5% of JVM mem
	private static final long CACHE_LIMIT; //limit in bytes
	private static String outdir = null;
	private static long _cachesize = 0;
	private static Entry _head = null;
	private static Entry _end = null;

	static {
		long maxMem = InfrastructureAnalyzer.getLocalMaxMemory();
		CACHE_LIMIT = (long)(CACHE_FRAC * maxMem);
	}
	
	//--------------------- CACHE LOGIC METHODS ----------------------
	
	public static boolean reuse(Instruction inst, ExecutionContext ec) {
		if (ReuseCacheType.isNone())
			return false;
		
		boolean reuse = false;
		//NOTE: the check for computation CP instructions ensures that the output
		// will always fit in memory and hence can be pinned unconditionally
		if (inst instanceof ComputationCPInstruction && LineageCache.isReusable(inst, ec)) {
			LineageItem item = ((ComputationCPInstruction) inst).getLineageItems(ec)[0];
			
			synchronized( _cache ) {
				//try to reuse full or partial intermediates
				if (LineageCacheConfig.getCacheType().isFullReuse())
					reuse = fullReuse(item, (ComputationCPInstruction)inst, ec); 
				if (LineageCacheConfig.getCacheType().isPartialReuse())
					reuse |= LineageRewriteReuse.executeRewrites(inst, ec);
				
				if (reuse && DMLScript.STATISTICS)
					LineageCacheStatistics.incrementInstHits();

				//create a placeholder if no reuse to avoid redundancy
				//(e.g., concurrent threads that try to start the computation)
				if( ! reuse )
					putIntern(item, null, 0);
			}
		}
		
		return reuse;
	}
	
	public static MatrixBlock reuse(LineageItem item) {
		if (ReuseCacheType.isNone())
			return null;

		MatrixBlock d = null;
		synchronized( _cache ) {
			if (LineageCache.probe(item)) 
				d = LineageCache.get(item);
			else
				//create a placeholder if no reuse to avoid redundancy
				//(e.g., concurrent threads that try to start the computation)
				putIntern(item, null, 0);
				//FIXME: parfor - every thread gets different function names
		}
		return d;
	}
	
	public static boolean reuse(List<String> outputs, int numOutputs, LineageItem[] liInputs, String name, ExecutionContext ec)
	{
		if( ReuseCacheType.isNone() )
			return false;

		boolean reuse = (numOutputs != 0);
		for (int i=0; i<numOutputs; i++) {
			String opcode = name + String.valueOf(i+1);
			LineageItem li = new LineageItem(outputs.get(i), opcode, liInputs);
			MatrixBlock cachedValue = LineageCache.reuse(li); 
			//TODO: handling of recursive calls
			
			if (cachedValue != null) {
				String boundVarName = outputs.get(i);
				//convert to matrix object
				MetaDataFormat md = new MetaDataFormat(cachedValue.getDataCharacteristics(), 
						OutputInfo.BinaryCellOutputInfo, InputInfo.BinaryCellInputInfo);
				MatrixObject boundValue = new MatrixObject(ValueType.FP64, boundVarName, md);
				boundValue.acquireModify(cachedValue);
				boundValue.release();

				//cleanup existing data bound to output variable name
				Data exdata = ec.removeVariable(boundVarName);
				if( exdata != boundValue)
					ec.cleanupDataObject(exdata);

				//add/replace data in symbol table
				ec.setVariable(boundVarName, boundValue);
				
				// map original lineage of function return to the calling site
				LineageItem orig = _cache.get(li)._origItem; //FIXME: synchronize
				ec.getLineage().set(boundVarName, orig);
			}
			else {
				// if one output cannot be reused, we need to execute the function
				// NOTE: all outputs need to be prepared for caching and hence,
				// we cannot directly return here
				reuse = false;
			}
		}
		return reuse;
	}
	
	//NOTE: safe to pin the object in memory as coming from CPInstruction
	public static void put(Instruction inst, ExecutionContext ec) {
		if (inst instanceof ComputationCPInstruction && isReusable(inst, ec) ) {
			LineageItem item = ((LineageTraceable) inst).getLineageItems(ec)[0];
			MatrixObject mo = ec.getMatrixObject(((ComputationCPInstruction) inst).output);
			synchronized( _cache ) {
				putIntern(item, mo.acquireReadAndRelease(), getRecomputeEstimate(inst, ec));
			}
		}
	}
	
	public static void putValue(Instruction inst, ExecutionContext ec) {
		if (ReuseCacheType.isNone())
			return;
		if (inst instanceof ComputationCPInstruction && isReusable(inst, ec) ) {
			LineageItem item = ((LineageTraceable) inst).getLineageItems(ec)[0];
			MatrixObject mo = ec.getMatrixObject(((ComputationCPInstruction) inst).output);
			MatrixBlock value = mo.acquireReadAndRelease();
			_cache.get(item).setValue(value, getRecomputeEstimate(inst, ec)); //outside sync to prevent deadlocks
			
			synchronized( _cache ) {
				if( !isBelowThreshold(value) ) 
					makeSpace(value);
				updateSize(value, true);
			}
		}
	}
	
	public static void putValue(LineageItem item, LineageItem probeItem) {
		if (ReuseCacheType.isNone())
			return;
		if (LineageCache.probe(probeItem)) {
			MatrixBlock value = LineageCache.get(probeItem);
			Entry e = _cache.get(item);
			e.setValue(value, 0); //TODO: compute estimate for function
			e._origItem = probeItem; 

			synchronized( _cache ) {
				if(!isBelowThreshold(value)) 
					makeSpace(value);
				updateSize(value, true);
			}
		}
		else
			removeEntry(item);  //remove the placeholder

	}

	public static void putValue(List<String> outputs, int numOutputs, LineageItem[] liInputs, String name, ExecutionContext ec)
	{
		if( ReuseCacheType.isNone() )
			return;

		HashMap<LineageItem, LineageItem> FuncLIMap = new HashMap<>();
		boolean AllOutputsCacheable = true;
		for (int i=0; i<numOutputs; i++) {
			String opcode = name + String.valueOf(i+1);
			LineageItem li = new LineageItem(outputs.get(i), opcode, liInputs);
			String boundVarName = outputs.get(i);
			LineageItem boundLI = ec.getLineage().get(boundVarName);
			Data boundValue = ec.getVariable(boundVarName);
			if (boundLI == null 
				|| !LineageCache.probe(li)
				|| LineageItemUtils.containsRandDataGen(new HashSet<>(Arrays.asList(liInputs)), boundLI)
				|| boundValue instanceof ScalarObject) { //TODO: cache scalar objects
				AllOutputsCacheable = false;
			}
			FuncLIMap.put(li, boundLI);
		}

		//cache either all the outputs, or none.
		if(AllOutputsCacheable) 
			FuncLIMap.forEach((Li, boundLI) -> LineageCache.putValue(Li, boundLI));
		else 
			//remove all the placeholders
			FuncLIMap.forEach((Li, boundLI) -> LineageCache.removeEntry(Li));
		
		return;
	}
	
	private static void putIntern(LineageItem key, MatrixBlock value, double compcost) {
		if (_cache.containsKey(key))
			//can come here if reuse_partial option is enabled
			return; 
			//throw new DMLRuntimeException("Redundant lineage caching detected: "+inst);
		
		// Create a new entry.
		Entry newItem = new Entry(key, value, compcost);
		
		// Make space by removing or spilling LRU entries.
		if( value != null ) {
			if( value.getInMemorySize() > CACHE_LIMIT )
				return; //not applicable
			if( !isBelowThreshold(value) ) 
				makeSpace(value);
			updateSize(value, true);
		}
		
		// Place the entry at head position.
		setHead(newItem);
		
		_cache.put(key, newItem);
		if (DMLScript.STATISTICS)
			LineageCacheStatistics.incrementMemWrites();
	}
	
	protected static boolean probe(LineageItem key) {
		boolean p = (_cache.containsKey(key) || _spillList.containsKey(key));
		if (!p && DMLScript.STATISTICS && _removelist.contains(key))
			// The sought entry was in cache but removed later 
			LineageCacheStatistics.incrementDelHits();
		return p;
	}
	
	public static void resetCache() {
		_cache.clear();
		_spillList.clear();
		_head = null;
		_end = null;
		if (DMLScript.STATISTICS)
			_removelist.clear();
	}
	

	private static boolean fullReuse (LineageItem item, ComputationCPInstruction inst, ExecutionContext ec) {
		if (LineageCache.probe(item)) {
			MatrixBlock d = LineageCache.get(item);
			ec.setMatrixOutput(inst.output.getName(), d);
			return true;
		}
		return false;
	}
	
	protected static MatrixBlock get(LineageItem key) {
		// This method is called only when entry is present either in cache or in local FS.
		if (_cache.containsKey(key)) {
			// Read and put the entry at head.
			Entry e = _cache.get(key);
			delete(e);
			setHead(e);
			if (DMLScript.STATISTICS)
				LineageCacheStatistics.incrementMemHits();
			return e.getValue();
		}
		else
			return readFromLocalFS(key);
	}
	
	public static boolean isReusable (Instruction inst, ExecutionContext ec) {
		// TODO: Move this to the new class LineageCacheConfig and extend
		return inst.getOpcode().equalsIgnoreCase("tsmm")
				|| inst.getOpcode().equalsIgnoreCase("ba+*")
				|| ((inst.getOpcode().equalsIgnoreCase("*") 
				|| inst.getOpcode().equalsIgnoreCase("/")) &&
					inst instanceof BinaryMatrixMatrixCPInstruction) //TODO support scalar
				|| inst.getOpcode().equalsIgnoreCase("rightIndex")
				|| inst.getOpcode().equalsIgnoreCase("groupedagg")
				|| inst.getOpcode().equalsIgnoreCase("r'")
				|| (inst.getOpcode().equalsIgnoreCase("append") && isVectorAppend(inst, ec))
				|| inst.getOpcode().equalsIgnoreCase("solve");
	}
	
	private static boolean isVectorAppend(Instruction inst, ExecutionContext ec) {
		ComputationCPInstruction cpinst = (ComputationCPInstruction) inst;
		if( !cpinst.input1.isMatrix() || !cpinst.input2.isMatrix() )
			return false;
		long c1 = ec.getMatrixObject(cpinst.input1).getNumColumns();
		long c2 = ec.getMatrixObject(cpinst.input2).getNumColumns();
		return(c1 == 1 || c2 == 1);
	}
	
	//---------------- CACHE SPACE MANAGEMENT METHODS -----------------
	
	private static boolean isBelowThreshold(MatrixBlock value) {
		return ((value.getInMemorySize() + _cachesize) <= CACHE_LIMIT);
	}
	
	private static void makeSpace(MatrixBlock value) {
		double valSize = value.getInMemorySize();
		// cost based eviction
		while ((valSize+_cachesize) > CACHE_LIMIT)
		{
			if (_cache.get(_end._key).isNullVal()) {
				setEnd2Head(_end);  // Must be null function entry. Move to next.
				continue;
			}
				
			double reduction = _cache.get(_end._key).getValue().getInMemorySize();
			if (_cache.get(_end._key)._compEst > getDiskSpillEstimate() 
					&& LineageCacheConfig.isSetSpill())
				spillToLocalFS(); // If re-computation is more expensive, spill data to disk.

			removeEntry(reduction);
		} 
	}
	
	private static void updateSize(MatrixBlock value, boolean addspace) {
		if (addspace)
			_cachesize += value.getInMemorySize();
		else
			_cachesize -= value.getInMemorySize();
	}

	//---------------- COSTING RELATED METHODS -----------------

	private static double getDiskSpillEstimate() {
		// This includes sum of writing to and reading from disk
		long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;
		MatrixBlock mb = _cache.get(_end._key).getValue();
		long r = mb.getNumRows();
		long c = mb.getNumColumns();
		long nnz = mb.getNonZeros();
		double s = OptimizerUtils.getSparsity(r, c, nnz);
		double loadtime = CostEstimatorStaticRuntime.getFSReadTime(r, c, s);
		double writetime = CostEstimatorStaticRuntime.getFSWriteTime(r, c, s);
		if (DMLScript.STATISTICS) 
			LineageCacheStatistics.incrementCostingTime(System.nanoTime() - t0);
		return loadtime+writetime;
	}
	
	private static double getRecomputeEstimate(Instruction inst, ExecutionContext ec) {
		long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;
		double nflops = 0;
		CPType cptype = CPInstructionParser.String2CPInstructionType.get(inst.getOpcode());
		//TODO: All other relevant instruction types.
		switch (cptype)
		{
			case MMTSJ:  //tsmm
			{
				MatrixObject mo = ec.getMatrixObject(((ComputationCPInstruction)inst).input1);
				long r = mo.getNumRows();
				long c = mo.getNumColumns();
				long nnz = mo.getNnz();
				double s = OptimizerUtils.getSparsity(r, c, nnz);
				boolean sparse = MatrixBlock.evalSparseFormatInMemory(r, c, nnz);
				MMTSJType type = ((MMTSJCPInstruction)inst).getMMTSJType();
				if (type.isLeft())
					nflops = !sparse ? (r * c * s * c /2):(r * c * s * c * s /2);
				else
					nflops = !sparse ? ((double)r * c * r/2):(r*c*s + r*c*s*c*s /2);
				break;
			}
				
			case AggregateBinary:  //ba+*
			{
				MatrixObject mo1 = ec.getMatrixObject(((ComputationCPInstruction)inst).input1);
				MatrixObject mo2 = ec.getMatrixObject(((ComputationCPInstruction)inst).input2);
				long r1 = mo1.getNumRows();
				long c1 = mo1.getNumColumns();
				long nnz1 = mo1.getNnz();
				double s1 = OptimizerUtils.getSparsity(r1, c1, nnz1);
				boolean lsparse = MatrixBlock.evalSparseFormatInMemory(r1, c1, nnz1);
				long r2 = mo2.getNumRows();
				long c2 = mo2.getNumColumns();
				long nnz2 = mo2.getNnz();
				double s2 = OptimizerUtils.getSparsity(r2, c2, nnz2);
				boolean rsparse = MatrixBlock.evalSparseFormatInMemory(r2, c2, nnz2);
				if( !lsparse && !rsparse )
					nflops = 2 * (r1 * c1 * ((c2>1)?s1:1.0) * c2) /2;
				else if( !lsparse && rsparse )
					nflops = 2 * (r1 * c1 * s1 * c2 * s2) /2;
				else if( lsparse && !rsparse )
					nflops = 2 * (r1 * c1 * s1 * c2) /2;
				else //lsparse && rsparse
					nflops = 2 * (r1 * c1 * s1 * c2 * s2) /2;
				break;
			}
				
			case Binary:  //*, /
			{
				MatrixObject mo1 = ec.getMatrixObject(((ComputationCPInstruction)inst).input1);
				long r1 = mo1.getNumRows();
				long c1 = mo1.getNumColumns();
				if (inst.getOpcode().equalsIgnoreCase("*") || inst.getOpcode().equalsIgnoreCase("/"))
					// considering the dimensions of inputs and the output are same 
					nflops = r1 * c1; 
				else if (inst.getOpcode().equalsIgnoreCase("solve"))
					nflops = r1 * c1 * c1;
				break;
			}
			
			case MatrixIndexing:  //rightIndex
			{
				MatrixObject mo1 = ec.getMatrixObject(((ComputationCPInstruction)inst).input1);
				long r1 = mo1.getNumRows();
				long c1 = mo1.getNumColumns();
				long nnz1 = mo1.getNnz();
				double s1 = OptimizerUtils.getSparsity(r1, c1, nnz1);
				boolean lsparse = MatrixBlock.evalSparseFormatInMemory(r1, c1, nnz1);
				if (inst.getOpcode().equalsIgnoreCase("rightIndex"))
					nflops = 1.0 * (lsparse ? r1 * c1 * s1 : r1 * c1); //FIXME
				break;
			}
			
			case ParameterizedBuiltin:  //groupedagg (sum, count)
			{
				String opcode = ((ParameterizedBuiltinCPInstruction)inst).getOpcode();
				HashMap<String, String> params = ((ParameterizedBuiltinCPInstruction)inst).getParameterMap();
				long r1 = ec.getMatrixObject(params.get(Statement.GAGG_TARGET)).getNumRows();
				String fn = params.get(Statement.GAGG_FN);
				double xga = 0;
				if (opcode.equalsIgnoreCase("groupedagg")) {
					if (fn.equalsIgnoreCase("sum"))
						xga = 4;
					else if(fn.equalsIgnoreCase("count"))
						xga = 1;
					//TODO: cm, variance
				}
				//TODO: support other PBuiltin ops
				nflops = 2 * r1+xga * r1;
				break;
			}

			case Reorg:  //r'
			{
				MatrixObject mo = ec.getMatrixObject(((ComputationCPInstruction)inst).input1);
				long r = mo.getNumRows();
				long c = mo.getNumColumns();
				long nnz = mo.getNnz();
				double s = OptimizerUtils.getSparsity(r, c, nnz);
				boolean sparse = MatrixBlock.evalSparseFormatInMemory(r, c, nnz);
				nflops = sparse ? r*c*s : r*c; 
				break;
			}

			case Append:  //cbind, rbind
			{
				MatrixObject mo1 = ec.getMatrixObject(((ComputationCPInstruction)inst).input1);
				MatrixObject mo2 = ec.getMatrixObject(((ComputationCPInstruction)inst).input2);
				long r1 = mo1.getNumRows();
				long c1 = mo1.getNumColumns();
				long nnz1 = mo1.getNnz();
				double s1 = OptimizerUtils.getSparsity(r1, c1, nnz1);
				boolean lsparse = MatrixBlock.evalSparseFormatInMemory(r1, c1, nnz1);
				long r2 = mo2.getNumRows();
				long c2 = mo2.getNumColumns();
				long nnz2 = mo2.getNnz();
				double s2 = OptimizerUtils.getSparsity(r2, c2, nnz2);
				boolean rsparse = MatrixBlock.evalSparseFormatInMemory(r2, c2, nnz2);
				nflops = 1.0 * ((lsparse ? r1*c1*s1 : r1*c1) + (rsparse ? r2*c2*s2 : r2*c2));
				break;
			}

			default:
				throw new DMLRuntimeException("Lineage Cache: unsupported instruction: "+inst.getOpcode());
		}
		
		if (DMLScript.STATISTICS) {
			long t1 = System.nanoTime();
			LineageCacheStatistics.incrementCostingTime(t1-t0);
		}
		return nflops / (2L * 1024 * 1024 * 1024);
	}

	// ---------------- I/O METHODS TO LOCAL FS -----------------
	
	private static void spillToLocalFS() {
		long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;
		if (outdir == null) {
			outdir = LocalFileUtils.getUniqueWorkingDir(LocalFileUtils.CATEGORY_LINEAGE);
			LocalFileUtils.createLocalFileIfNotExist(outdir);
		}
		String outfile = outdir+"/"+_cache.get(_end._key)._key.getId();
		try {
			LocalFileUtils.writeMatrixBlockToLocal(outfile, _cache.get(_end._key).getValue());
		} catch (IOException e) {
			throw new DMLRuntimeException ("Write to " + outfile + " failed.", e);
		}
		if (DMLScript.STATISTICS) {
			long t1 = System.nanoTime();
			LineageCacheStatistics.incrementFSWriteTime(t1-t0);
			LineageCacheStatistics.incrementFSWrites();
		}

		_spillList.put(_end._key, new SpilledItem(outfile, _end._compEst));
	}
	
	private static MatrixBlock readFromLocalFS(LineageItem key) {
		long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;
		MatrixBlock mb = null;
		// Read from local FS
		try {
			mb = LocalFileUtils.readMatrixBlockFromLocal(_spillList.get(key)._outfile);
		} catch (IOException e) {
			throw new DMLRuntimeException ("Read from " + _spillList.get(key)._outfile + " failed.", e);
		}
		// Restore to cache
		LocalFileUtils.deleteFileIfExists(_spillList.get(key)._outfile, true);
		putIntern(key, mb, _spillList.get(key)._compEst);
		_spillList.remove(key);
		if (DMLScript.STATISTICS) {
			long t1 = System.nanoTime();
			LineageCacheStatistics.incrementFSReadTime(t1-t0);
			LineageCacheStatistics.incrementFSHits();
		}
		return mb;
	}

	//------------------ LINKEDLIST MAINTENANCE METHODS -------------------
	
	private static void delete(Entry entry) {
		if (entry._prev != null)
			entry._prev._next = entry._next;
		else
			_head = entry._next;
		if (entry._next != null)
			entry._next._prev = entry._prev;
		else
			_end = entry._prev;
	}
	
	private static void setHead(Entry entry) {
		entry._next = _head;
		entry._prev = null;
		if (_head != null)
			_head._prev = entry;
		_head = entry;
		if (_end == null)
			_end = _head;
	}
	
	private static void setEnd2Head(Entry entry) {
		delete(entry);
		setHead(entry);
	}

	private static void removeEntry(double space) {
		if (DMLScript.STATISTICS)
			_removelist.add(_end._key);
		_cache.remove(_end._key);
		_cachesize -= space;
		delete(_end);
	}
	
	public static void removeEntry(LineageItem key) {
		// Remove the entry for key
		if (!_cache.containsKey(key))
			return;
		delete(_cache.get(key));
		_cache.remove(key);
	}
	
	private static class Entry {
		private final LineageItem _key;
		private MatrixBlock _val;
		double _compEst;
		private Entry _prev;
		private Entry _next;
		private LineageItem _origItem;
		
		public Entry(LineageItem key, MatrixBlock value, double computecost) {
			_key = key;
			_val = value;
			_compEst = computecost;
			_origItem = null;
		}

		public synchronized MatrixBlock getValue() {
			try {
				//wait until other thread completes operation
				//in order to avoid redundant computation
				while( _val == null ) {
					wait();
				}
				return _val;
			}
			catch( InterruptedException ex ) {
				throw new DMLRuntimeException(ex);
			}
		}
		
		public boolean isNullVal() {
			return(_val == null);
		}
		
		public synchronized void setValue(MatrixBlock val, double compEst) {
			_val = val;
			_compEst = compEst;
			notifyAll();
		}
	}
	
	private static class SpilledItem {
		String _outfile;
		double _compEst;

		public SpilledItem(String outfile, double computecost) {
			this._outfile = outfile;
			this._compEst = computecost;
		}
	}
}
