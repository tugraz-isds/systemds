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

package org.tugraz.sysds.test.functions.transform;

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
	
	// Set input frame
	@BeforeClass
	public static void init() {
		ValueType[] schema = new ValueType[] {ValueType.STRING, ValueType.STRING, ValueType.INT32, ValueType.FP64};
		ori = new FrameBlock(schema);
		String[] strings = new String[] { 
				  "Toyota", "Mercedes", "BMW", "Volkswagen", "Skoda" };
		String[] strings2 = new String[] { 
				  "Austria", "Spain", "France", "United Kingdom"};
		Random rand = new Random();
		
		for(int i = 0; i<1000; i++) {
			Object[] row = new Object[4];
			row[0] = strings[rand.nextInt(strings.length)];
			row[1] = strings2[rand.nextInt(strings2.length)];
			row[2] = rand.nextInt(1000);
			row[3] = rand.nextGaussian();
			
			ori.appendRow(row);
		}
	}
	
	// Reset lists and output frame
	@Before
	public void testInit() {
		numerics = new ArrayList<Integer>();
		stringpos = new ArrayList<Integer>();
		swappable = new ArrayList<Integer>();
		changed = DataAugmentation.preprocessing(ori, numerics, stringpos, swappable);
	}
	
	@Test
	public void testTypos() {
		changed = DataAugmentation.typos(changed, stringpos, 0.2);
		
		double numch = 0.;
		for(int i=0;i<changed.getNumRows();i++) {
			String label = (String) changed.get(i, changed.getNumColumns()-1);
			if(label.equals("typo")) {
				numch++;
				boolean checkTypo = false;
				for(int c:stringpos) {
					String val = (String) ori.get(i, c);
					String valch = (String) changed.get(i, c);
					checkTypo = checkTypo || !val.equals(valch);
					if(checkTypo) break;
				}
				assertTrue("Row "+i+" was marked with typos, but it has not been changed.", checkTypo);
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
					if((ori.get(i, c)!=null && !ori.get(i, c).equals(0) && !ori.get(i, c).equals(0d) && !ori.get(i, c).equals(0L) && !ori.get(i, c).equals(0f)) && 
							(changed.get(i, c)==null || changed.get(i, c).equals(0) || changed.get(i, c).equals(0d) || changed.get(i, c).equals(0L) || changed.get(i, c).equals(0f))) dropped++;
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
				boolean checkOut = false;
				for(int c:numerics) {
					if(changed.getSchema()[c].equals(ValueType.INT32)) {
						Integer val = (Integer) ori.get(i, c);
						Integer valch = (Integer) changed.get(i, c);
						checkOut = checkOut || !val.equals(valch);
					} else if(changed.getSchema()[c].equals(ValueType.INT64)) {
						Long val = (Long) ori.get(i, c);
						Long valch = (Long) changed.get(i, c);
						checkOut = checkOut || !val.equals(valch);
					} else if(changed.getSchema()[c].equals(ValueType.FP32)) {
						Float val = (Float) ori.get(i, c);
						Float valch = (Float) changed.get(i, c);
						checkOut = checkOut || !val.equals(valch);
					} else if(changed.getSchema()[c].equals(ValueType.FP64)) {
						Double val = (Double) ori.get(i, c);
						Double valch = (Double) changed.get(i, c);
						checkOut = checkOut || !val.equals(valch);
					}
					if(checkOut) break;
				}
				assertTrue("Row "+i+" was marked with outliers, but it has not been changed.", checkOut);
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
