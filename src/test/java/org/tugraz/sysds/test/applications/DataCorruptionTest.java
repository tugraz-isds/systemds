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

package org.tugraz.sysds.test.applications;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.utils.DataAugmentation;

public class DataCorruptionTest{

	private static FrameBlock ori;
	private FrameBlock changed;
	private List<Integer> numerics;
	private List<Integer> stringpos;
	private List<Integer> swappable;
	
	@BeforeClass
	public static void init() {
		ValueType[] schema = new ValueType[] {ValueType.STRING, ValueType.INT32};
		ori = new FrameBlock(schema);
		String[] strings = new String[] { 
				  "Toyota", "Mercedes", "BMW", "Volkswagen", "Skoda" };
		Random rand = new Random();
		
		for(int i = 0; i<1000; i++) {
			Object[] row = new Object[2];
			row[0] = strings[rand.nextInt(strings.length)];
			row[1] = rand.nextInt(1000);
			
			ori.appendRow(row);
		}
	}
	
	@Before
	public void testInit() {
		numerics = new ArrayList<Integer>();
		stringpos = new ArrayList<Integer>();
		swappable = new ArrayList<Integer>();
		changed = DataAugmentation.preprocessing(ori, numerics, stringpos, swappable);
	}
	// Split into multiple test
	
//	@Test
//	public void testDataCorruption() {
//		ValueType[] schema = new ValueType[] {ValueType.STRING, ValueType.INT32};
//		FrameBlock ori = new FrameBlock(schema);
//		String[] strings = new String[] { 
//				  "Toyota", "Mercedes", "BMW", "Volkswagen", "Skoda" };
//		Random rand = new Random();
//		
//		for(int i = 0; i<1000; i++) {
//			Object[] row = new Object[2];
//			row[0] = strings[rand.nextInt(strings.length)];
//			row[1] = rand.nextInt(10000);
//			
//			ori.appendRow(row);
//		}
//		
//		FrameBlock changed = DataAugmentation.dataCorruption(ori, 0.2, 0.4, 0.8, 0.6);
//		
//		for(int i = 0; i<1000; i++) {
//			switch((String) changed.get(i, changed.getNumColumns()-1)) {
//			case "typo":
//				String or = (String) ori.get(i, 0);
//				String ch = (String) changed.get(i, 0);
//				System.out.println(ori.get(i, 0) + " -> " + changed.get(i, 0));
//				assertNotEquals("Typo test", or, ch);
//				break;
//			case "missing":
//				break;
//			case "outlier":
//				Integer nor = (Integer) ori.get(i, 1);
//				Integer nch = (Integer) changed.get(i, 1);
//				assertTrue(nch/nor==100);
//			}
//		}
//	}
	
	@Test
	public void testTypos() {
		changed = DataAugmentation.typos(changed, stringpos, 0.2);
		
		double numch = 0.;
		for(int i=0;i<changed.getNumRows();i++) {
			String label = (String) changed.get(i, changed.getNumColumns()-1);
			if(label.equals("typo")) {
				numch++;
				String val = (String) ori.get(i, 0);
				String valch = (String) changed.get(i, 0);
				assertNotEquals("Row "+i+" was marked with typos, but it has not been changed.", val, valch);
			}
		}
		System.out.println("Test typos: number of changed rows: " + numch);
		assertEquals("The number of changed rows is not approx. 20%", 0.2, numch/changed.getNumRows(), 0.05);
		
	}
	
	@Test
	public void testMiss() {
		changed = DataAugmentation.miss(changed, 0.2, 0.7);
		
		double numch = 0.;
		for(int i=0;i<changed.getNumRows();i++) {
			String label = (String) changed.get(i, changed.getNumColumns()-1);
			if(label.equals("missing")) {
				int dropped = 0;
				for(int c=0;c<ori.getNumColumns();c++) {
					if((ori.get(i, c)!=null && !ori.get(i, c).equals(0)) && (changed.get(i, c)==null || changed.get(i, c).equals(0))) dropped++;
				}
				numch++;
				assertTrue("Row "+i+" was marked with missing values, but it has not been changed.", dropped>0);
			}
		}
		System.out.println("Test missing: number of changed rows: " + numch);
		assertEquals("The number of changed rows is not approx. 20%", 0.2, numch/changed.getNumRows(), 0.05);
	}
	
	@Test
	public void testOutliers() {
		changed = DataAugmentation.outlier(changed, numerics, 0.2, 0.5, 3);
		
		double numch = 0.;
		for(int i=0;i<changed.getNumRows();i++) {
			String label = (String) changed.get(i, changed.getNumColumns()-1);
			if(label.equals("outlier")) {
				numch++;
				Integer val = (Integer) ori.get(i, 1);
				Integer valch = (Integer) changed.get(i, 1);
				assertNotEquals("Row "+i+" was marked with outliers, but it has not been changed.", val, valch);
			}
		}
		System.out.println("Test outliers: number of changed rows: " + numch);
		assertEquals("The number of changed rows is not approx. 20%", 0.2, numch/changed.getNumRows(), 0.05);
	}
	
	@Test
	public void testSwap() {
		ValueType[] schema = new ValueType[] {ValueType.INT32, ValueType.INT32, ValueType.INT32};
		FrameBlock ori = new FrameBlock(schema);
		
		Random rand = new Random();
		
		for(int i = 0; i<1000; i++) {
			Object[] row = new Object[3];
			row[0] = rand.nextInt(1000);
			row[1] = rand.nextInt(1000);
			row[2] = rand.nextInt(1000);
			
			ori.appendRow(row);
		}
		
		List<Integer> numerics = new ArrayList<Integer>();
		List<Integer> strings = new ArrayList<Integer>();
		List<Integer> swappable = new ArrayList<Integer>();
		
		FrameBlock changed = DataAugmentation.preprocessing(ori, numerics, strings, swappable);
		
		changed = DataAugmentation.swap(changed, swappable, 0.2);
		
		double numch = 0.;
		for(int i=0;i<changed.getNumRows();i++) {
			String label = (String) changed.get(i, changed.getNumColumns()-1);
			if(label.equals("swap")) {
				numch++;
				boolean checkSwap = false;
				for(int c:swappable) {
					Integer val0 = (Integer) ori.get(i, c);
					Integer valch0 = (Integer) changed.get(i, c);
					Integer val1 = (Integer) ori.get(i, c+1);
					Integer valch1 = (Integer) changed.get(i, c+1);
					checkSwap = checkSwap || (val0.equals(valch1) && val1.equals(valch0));
					if(checkSwap) break;
				}
				assertTrue("Row "+i+" was marked with outliers, but it has not been changed.",checkSwap);
			}
		}
		System.out.println("Test swap: number of changed rows: " + numch);
		assertEquals("The number of changed rows is not approx. 20%", 0.2, numch/changed.getNumRows(), 0.05);
		
	}
}
