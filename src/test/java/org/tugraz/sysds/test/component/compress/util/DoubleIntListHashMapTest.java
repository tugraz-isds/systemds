package org.tugraz.sysds.test.component.compress.util;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;

import org.junit.Test;
import org.tugraz.sysds.runtime.compress.utils.DoubleIntListHashMap;
import org.tugraz.sysds.runtime.compress.utils.DoubleIntListHashMap.DIListEntry;
import org.tugraz.sysds.runtime.compress.utils.IntArrayList;

public class DoubleIntListHashMapTest {

	@Test
	public void constructor() {
		DoubleIntListHashMap m = new DoubleIntListHashMap();
		assertTrue("The number of elements in a new hashmap should be 0", m.size() == 0);
	}

	@Test
	public void addAndRemoveTest() {
		DoubleIntListHashMap m = new DoubleIntListHashMap();
		Double mA = 0.01;
		IntArrayList mAL = mock(IntArrayList.class);
		m.appendValue(mA, mAL);
		IntArrayList rAL = m.get(mA);
		assertTrue("The get array should be the same as input", mAL == rAL);
	}

	@Test
	public void extractValuesOfSingle() {
		try {

			DoubleIntListHashMap m = new DoubleIntListHashMap();
			Double mA = 0.01;
			IntArrayList mAL = mock(IntArrayList.class);

			m.appendValue(mA, mAL);

			ArrayList<DIListEntry> extracted = m.extractValues();

			ArrayList<Double> keys = new ArrayList<>();
			ArrayList<IntArrayList> values = new ArrayList<>();

			for(DIListEntry ent : extracted) {
				keys.add(ent.key);
				values.add(ent.value);
			}

			assertTrue("extract values should return the input key", keys.get(0).equals(mA));
			assertTrue("extract values should return the input value", values.get(0).equals( mAL));
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void extractValuesOfMany() {
		try {

			DoubleIntListHashMap m = new DoubleIntListHashMap();
			Double mA1 = 0.1;
			Double mA2 = 0.3;
			Double mA3 = 0.5;
			Double mA4 = 1.1;
			IntArrayList mAL1 = new IntArrayList(1);
			IntArrayList mAL2 = new IntArrayList(2);
			IntArrayList mAL3 = new IntArrayList(3);
			IntArrayList mAL4 = new IntArrayList(4);
			IntArrayList mAL5 = new IntArrayList(5);

			m.appendValue(mA1, mAL1);
			m.appendValue(mA2, mAL2);
			m.appendValue(mA3, mAL3);
			m.appendValue(mA4, mAL4);
			m.appendValue(mA4, mAL5);

			ArrayList<DIListEntry> extracted = m.extractValues();

			ArrayList<Double> keys = new ArrayList<>();
			ArrayList<IntArrayList> values = new ArrayList<>();

			for(DIListEntry ent : extracted) {
				keys.add(ent.key);
				values.add(ent.value);
			}

			assertTrue("result contains first element", values.contains(mAL1));
			assertTrue("result does not contain overwritten element", !values.contains(mAL4));
			assertTrue("contains 4 keys", keys.size() == 4);
			assertTrue("contains 4 values", values.size() == 4);
			assertTrue("reported size matches", m.size() == 4);
		}
		catch(Exception e) {
			e.printStackTrace();
			assertTrue(e.toString(), false);
		}
	}

	@Test
	public void replaceValue() {
		DoubleIntListHashMap m = new DoubleIntListHashMap();
		Double mA1 = 0.1;
		IntArrayList mAL1 = new IntArrayList(2);
		IntArrayList mAL2 = new IntArrayList(4);

		m.appendValue(mA1, mAL1);
		m.appendValue(mA1, mAL2);

		ArrayList<DIListEntry> extracted = m.extractValues();

		ArrayList<Double> keys = new ArrayList<>();
		ArrayList<IntArrayList> values = new ArrayList<>();

		for(DIListEntry ent : extracted) {
			keys.add(ent.key);
			values.add(ent.value);
		}

		assertTrue("result contains the new element", values.contains(mAL2));
		assertTrue("result does not contain overwritten element", !values.contains(mAL1));
		assertTrue("contains 1 keys", keys.size() == 1);
		assertTrue("contains 1 values", values.size() == 1);
		assertTrue("reported size matches", m.size() == 1);
	}

}