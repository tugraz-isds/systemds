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

import java.util.Random;

import org.junit.Test;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.utils.DataAugmentation;

public class DataCorruptionTest{

	// Split into multiple test
	@Test
	public void testDataCorruption() {
		ValueType[] schema = new ValueType[] {ValueType.STRING, ValueType.INT32};
		FrameBlock ori = new FrameBlock(schema);
		String[] strings = new String[] { 
				  "Toyota", "Mercedes", "BMW", "Volkswagen", "Skoda" };
		Random rand = new Random();
		
		for(int i = 0; i<1000; i++) {
			Object[] row = new Object[2];
			row[0] = strings[rand.nextInt(strings.length)];
			row[1] = rand.nextInt(10000);
			
			ori.appendRow(row);
		}
		
		FrameBlock changed = DataAugmentation.dataCorruption(ori, 0.2, 0.4, 0.8, 0.6);
		
		for(int i = 0; i<1000; i++) {
			switch((String) changed.get(i, changed.getNumColumns()-1)) {
			case "typo":
				String or = (String) ori.get(i, 0);
				String ch = (String) changed.get(i, 0);
				System.out.println(ori.get(i, 0) + " -> " + changed.get(i, 0));
				assertNotEquals("Typo test", or, ch);
				break;
			case "missing":
				break;
			case "outlier":
				Integer nor = (Integer) ori.get(i, 1);
				Integer nch = (Integer) changed.get(i, 1);
				assertTrue(nch/nor==100);
			}
		}
	}
}
