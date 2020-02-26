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

import java.util.concurrent.atomic.LongAdder;

/**
 * This singleton provides basic lineage caching statistics in CP.
 * Hit statistics for caching (mem, fs, total)
 */
public class LineageCacheStatistics {
	private static final LongAdder _numHitsMem      = new LongAdder();
	private static final LongAdder _numHitsFS       = new LongAdder();
	private static final LongAdder _numHitsDel      = new LongAdder();
	private static final LongAdder _numHitsInst     = new LongAdder();
	private static final LongAdder _numHitsSB       = new LongAdder();
	private static final LongAdder _numHitsFunc     = new LongAdder();
	private static final LongAdder _numWritesMem    = new LongAdder();
	private static final LongAdder _numWritesFS     = new LongAdder();
	private static final LongAdder _numRewrites     = new LongAdder();
	private static final LongAdder _ctimeFSRead     = new LongAdder(); //in nano sec
	private static final LongAdder _ctimeFSWrite    = new LongAdder(); //in nano sec
	private static final LongAdder _ctimeCosting    = new LongAdder(); //in nano sec
	private static final LongAdder _ctimeRewrite    = new LongAdder(); //in nano sec
	private static final LongAdder _ctimeRewriteEx  = new LongAdder(); //in nano sec

	public static void reset() {
		_numHitsMem.reset();
		_numHitsFS.reset();
		_numHitsDel.reset();
		_numHitsInst.reset();
		_numHitsSB.reset();
		_numHitsFunc.reset();
		_numWritesMem.reset();
		_numWritesFS.reset();
		_numRewrites.reset();
		_ctimeFSRead.reset();
		_ctimeFSWrite.reset();
		_ctimeCosting.reset();
		_ctimeRewrite.reset();
		_ctimeRewriteEx.reset();
	}
	
	public static void incrementMemHits() {
		// Number of times found in cache.
		_numHitsMem.increment();
	}

	public static void incrementFSHits() {
		// Number of times found in local FS.
		_numHitsFS.increment();
	}

	public static void incrementDelHits() {
		// Number of times entry is removed from cache but sought again later.
		_numHitsDel.increment();
	}

	public static void incrementInstHits() {
		// Number of times single instruction results are reused (full and partial).
		_numHitsInst.increment();
	}

	public static void incrementSBHits() {
		// Number of times statementblock results are reused.
		_numHitsSB.increment();
	}

	public static void incrementFuncHits() {
		// Number of times function results are reused.
		_numHitsFunc.increment();
	}

	public static void incrementMemWrites() {
		// Number of times written in cache.
		_numWritesMem.increment();
	}

	public static void incrementPRewrites() {
		// Number of times written in local FS.
		_numRewrites.increment();
	}

	public static void incrementFSWrites() {
		// Number of times written in local FS.
		_numWritesFS.increment();
	}

	public static void incrementFSReadTime(long delta) {
		// Total time spent on reading from FS.
		_ctimeFSRead.add(delta);
	}

	public static void incrementFSWriteTime(long delta) {
		// Total time spent writing to FS.
		_ctimeFSWrite.add(delta);
	}

	public static void incrementCostingTime(long delta) {
		// Total time spent estimating computation and disk spill costs.
		_ctimeCosting.add(delta);
	}

	public static void incrementPRewriteTime(long delta) {
		// Total time spent executing lineage rewrites.
		_ctimeRewrite.add(delta);
	}

	public static void incrementPRwExecTime(long delta) {
		// Total time spent executing lineage rewrites.
		_ctimeRewriteEx.add(delta);
	}
	
	public static String displayHits() {
		StringBuilder sb = new StringBuilder();
		sb.append(_numHitsMem.longValue());
		sb.append("/");
		sb.append(_numHitsFS.longValue());
		sb.append("/");
		sb.append(_numHitsDel.longValue());
		return sb.toString();
	}

	public static String displayMultiLvlHits() {
		StringBuilder sb = new StringBuilder();
		sb.append(_numHitsInst.longValue());
		sb.append("/");
		sb.append(_numHitsSB.longValue());
		sb.append("/");
		sb.append(_numHitsFunc.longValue());
		return sb.toString();
	}

	public static String displayWtrites() {
		StringBuilder sb = new StringBuilder();
		sb.append(_numWritesMem.longValue());
		sb.append("/");
		sb.append(_numWritesFS.longValue());
		return sb.toString();
	}

	public static String displayRewrites() {
		StringBuilder sb = new StringBuilder();
		sb.append(_numRewrites.longValue());
		return sb.toString();
	}
	
	public static String displayTime() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("%.3f", ((double)_ctimeFSRead.longValue())/1000000000)); //in sec
		sb.append("/");
		sb.append(String.format("%.3f", ((double)_ctimeFSWrite.longValue())/1000000000)); //in sec
		return sb.toString();
	}
	
	public static String displayCostingTime() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("%.3f", ((double)_ctimeCosting.longValue())/1000000000)); //in sec
		return sb.toString();
	}

	public static String displayRewriteTime() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("%.3f", ((double)_ctimeRewrite.longValue())/1000000000)); //in sec
		sb.append("/");
		sb.append(String.format("%.3f", ((double)_ctimeRewriteEx.longValue())/1000000000)); //in sec
		return sb.toString();
	}
}
