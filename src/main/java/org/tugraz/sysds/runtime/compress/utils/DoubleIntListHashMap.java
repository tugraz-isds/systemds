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
 * This class provides a memory-efficient replacement for
 * {@code HashMap<Double,IntArrayList>} for restricted use cases.
 * 
 */
public class DoubleIntListHashMap 
{
	private static final int INIT_CAPACITY = 8;
	private static final int RESIZE_FACTOR = 2;
	private static final float LOAD_FACTOR = 0.75f;

	private DIListEntry[] _data = null;
	private int _size = -1;

	public DoubleIntListHashMap() {
		_data = new DIListEntry[INIT_CAPACITY];
		_size = 0;
	}

	public int size() {
		return _size;
	}

	public IntArrayList get(double key) {
		// probe for early abort
		if( _size == 0 )
			return null;

		// compute entry index position
		int hash = hash(key);
		int ix = indexFor(hash, _data.length);

		// find entry
		for( DIListEntry e = _data[ix]; e != null; e = e.next ) {
			if( e.key == key ) {
				return e.value;
			}
		}

		return null;
	}

	public void appendValue(double key, IntArrayList value) {
		// compute entry index position
		int hash = hash(key);
		int ix = indexFor(hash, _data.length);

		// add new table entry (constant time)
		DIListEntry enew = new DIListEntry(key, value);
		enew.next = _data[ix]; // colliding entries / null
		_data[ix] = enew;
		_size++;

		// resize if necessary
		if( _size >= LOAD_FACTOR * _data.length )
			resize();
	}

	public ArrayList<DIListEntry> extractValues() {
		ArrayList<DIListEntry> ret = new ArrayList<>();
		for( DIListEntry e : _data ) {
			if (e != null) {
				while( e.next != null ) {
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
		if( _data.length > Integer.MAX_VALUE / RESIZE_FACTOR )
			return;

		// resize data array and copy existing contents
		DIListEntry[] olddata = _data;
		_data = new DIListEntry[_data.length * RESIZE_FACTOR];
		_size = 0;

		// rehash all entries
		for( DIListEntry e : olddata ) {
			if( e != null ) {
				while( e.next != null ) {
					appendValue(e.key, e.value);
					e = e.next;
				}
				appendValue(e.key, e.value);
			}
		}
	}

	private static int hash(double key) {
		// basic double hash code (w/o object creation)
		long bits = Double.doubleToRawLongBits(key);
		int h = (int) (bits ^ (bits >>> 32));

		// This function ensures that hashCodes that differ only by
		// constant multiples at each bit position have a bounded
		// number of collisions (approximately 8 at default load factor).
		h ^= (h >>> 20) ^ (h >>> 12);
		return h ^ (h >>> 7) ^ (h >>> 4);
	}

	private static int indexFor(int h, int length) {
		return h & (length - 1);
	}

	public class DIListEntry {
		public double key = Double.MAX_VALUE;
		public IntArrayList value = null;
		public DIListEntry next = null;

		public DIListEntry(double ekey, IntArrayList evalue) {
			key = ekey;
			value = evalue;
			next = null;
		}
	}
}
