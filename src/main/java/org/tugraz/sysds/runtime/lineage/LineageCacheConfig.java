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
import java.util.ArrayList;

public class LineageCacheConfig {
	public enum cacheType {
		FULL,   // no rewrites
		PARTIAL, 
		BOTH,
		NONE
	}
	
	public ArrayList<String> _MMult = new ArrayList<String>();
	
	public enum cachedItemsL1 {
		TSMM,
		ALL
	}
	
	public enum cachedItemsL2 {
		CBIND,
		RBIND,
		INDEX,
		ALL
	}

	private static cacheType _cacheType = null;
	private static cachedItemsL1 _itemL1 = null;
	private static cachedItemsL2 _itemL2 = null;
	
	public static void setConfigTsmmCbind(cacheType ct) {
		_cacheType = ct;
		_itemL1 = cachedItemsL1.TSMM;
		_itemL2 = cachedItemsL2.CBIND;
	}
	
	public static void setConfig(cacheType ct, cachedItemsL1 itl1, cachedItemsL2 itl2) {
		_cacheType = ct;
		_itemL1 = itl1;
		_itemL2 = itl2;
	}
	
	public static void shutdownReuse() {
		DMLScript.LINEAGE = false;
		DMLScript.LINEAGE_REUSE = false;
	}

	public static void restartReuse() {
		DMLScript.LINEAGE = true;
		DMLScript.LINEAGE_REUSE = true;
	}
	
	public static cacheType getCacheType() {
		return _cacheType;
	}

	public static cachedItemsL1 getCachedItemL1() {
		return _itemL1;
	}

	public static cachedItemsL2 getCachedItemL2() {
		return _itemL2;
	}
}
