/*
 * Modifications Copyright 2020 Graz University of Technology
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

package org.tugraz.sysds.runtime.compress.utils;

import java.util.ArrayList;

/**
 * This class provides a memory-efficient replacement for {@code HashMap<DblArray,IntArrayList>} for restricted use
 * cases.
 * 
 * TODO: Fix allocation of size such that it contains some amount of overhead from the start, to enable hashmap
 * performance.
 */
public class DblArrayIntListHashMap extends CustomHashMap {

	private DArrayIListEntry[] _data = null;

	public DblArrayIntListHashMap() {
		_data = new DArrayIListEntry[INIT_CAPACITY];
		_size = 0;
	}

	public DblArrayIntListHashMap(int init_capacity) {
		_data = new DArrayIListEntry[init_capacity];
		_size = 0;
	}

	public IntArrayList get(DblArray key) {
		// probe for early abort
		if(_size == 0)
			return null;

		// compute entry index position
		int hash = hash(key);
		int ix = indexFor(hash, _data.length);

		// find entry
		for(DArrayIListEntry e = _data[ix]; e != null; e = e.next) {
			if(e.key.equals(key)) {
				return e.value;
			}
		}

		return null;
	}

	public void appendValue(DblArray key, IntArrayList value) {
		// compute entry index position
		int hash = hash(key);
		int ix = indexFor(hash, _data.length);

		// add new table entry (constant time)
		DArrayIListEntry enew = new DArrayIListEntry(key, value);
		enew.next = _data[ix]; // colliding entries / null
		_data[ix] = enew;
		if(enew.next != null && enew.next.key == key) {
			enew.next = enew.next.next;
			_size--;
		}
		_size++;

		// resize if necessary
		if(_size >= LOAD_FACTOR * _data.length)
			resize();
	}

	public ArrayList<DArrayIListEntry> extractValues() {
		ArrayList<DArrayIListEntry> ret = new ArrayList<>();
		for(DArrayIListEntry e : _data) {
			if(e != null) {
				while(e.next != null) {
					ret.add(e);
					e = e.next;
				}
				ret.add(e);
			}
		}

		return ret;
	}

	private void resize() {
		// check for integer overflow on resize
		if(_data.length > Integer.MAX_VALUE / RESIZE_FACTOR)
			return;

		// resize data array and copy existing contents
		DArrayIListEntry[] olddata = _data;
		_data = new DArrayIListEntry[_data.length * RESIZE_FACTOR];
		_size = 0;

		// rehash all entries
		for(DArrayIListEntry e : olddata) {
			if(e != null) {
				while(e.next != null) {
					appendValue(e.key, e.value);
					e = e.next;
				}
				appendValue(e.key, e.value);
			}
		}
	}

	private static int hash(DblArray key) {
		int h = key.hashCode();

		// This function ensures that hashCodes that differ only by
		// constant multiples at each bit position have a bounded
		// number of collisions (approximately 8 at default load factor).
		h ^= (h >>> 20) ^ (h >>> 12);
		return h ^ (h >>> 7) ^ (h >>> 4);
	}

	private static int indexFor(int h, int length) {
		return h & (length - 1);
	}

	public class DArrayIListEntry {
		public DblArray key;
		public IntArrayList value;
		public DArrayIListEntry next;

		public DArrayIListEntry(DblArray ekey, IntArrayList evalue) {
			key = ekey;
			value = evalue;
			next = null;
		}
	}
}
