/*
 * Copyright 2020 Graz University of Technology
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

package org.tugraz.sysds.test.component.compress.estim;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openjdk.jol.datamodel.X86_64_DataModel;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.FieldLayout;
import org.openjdk.jol.layouters.HotSpotLayouter;
import org.openjdk.jol.layouters.Layouter;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroup;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupDDC;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupDDC1;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupDDC2;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupOLE;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupOffset;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupRLE;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupSizes;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupUncompressed;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupValue;
import org.tugraz.sysds.runtime.data.DenseBlockFP64;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;

@RunWith(value = Parameterized.class)
public class JolEstimateTest {

	@Parameters
	public static Collection<Object[]> data() {
		ArrayList<Object[]> tests = new ArrayList<>();

		// Only add a single selected test of constructor with no compression
		tests.add(new Object[] {ColGroupUncompressed.class});
		tests.add(new Object[] {ColGroup.class});
		tests.add(new Object[] {ColGroupValue.class});
		tests.add(new Object[] {ColGroupOLE.class});
		tests.add(new Object[] {ColGroupDDC.class});
		tests.add(new Object[] {ColGroupDDC1.class});
		tests.add(new Object[] {ColGroupDDC2.class});
		tests.add(new Object[] {ColGroupRLE.class});
		tests.add(new Object[] {ColGroupOffset.class});

		return tests;
	}

	protected final Class<?> colGroupClass;
	private Layouter l;

	public JolEstimateTest(Class<?> colGroupClass) {
		this.colGroupClass = colGroupClass;
	}

	@Test
	public void estimate() {
		try {
			long estimate = ColGroupSizes.getEmptyMemoryFootprint(colGroupClass);
			long jolEstimate = getWorstCaseMemory(colGroupClass);
			assertTrue(
				"Memory Estimate of " + estimate + " Incorrect compared to " + jolEstimate + "\n"
					+ printWorstCaseMemoryEstimate(colGroupClass),
				estimate == jolEstimate);
		}
		catch(Exception e) {
			e.printStackTrace();
			assertTrue("Test Failed", false);
		}
	}

	private String printWorstCaseMemoryEstimate(Class<?> klass) {
		StringBuilder sb = new StringBuilder();
		l = new HotSpotLayouter(new X86_64_DataModel());
		sb.append("***** " + l);
		sb.append(ClassLayout.parseClass(klass, l).toPrintable());
		for(FieldLayout fl : ClassLayout.parseClass(klass, l).fields()) {
			if(fl.typeClass() == "org.tugraz.sysds.runtime.matrix.data.MatrixBlock") {
				sb.append(ClassLayout.parseClass(MatrixBlock.class, l).toPrintable());
				sb.append(ClassLayout.parseClass(DenseBlockFP64.class, l).toPrintable());
			}
		}
		return sb.toString();
	}

	// private String thisJVMMemoryEstimate(Class<?> klass) {
	// StringBuilder sb = new StringBuilder();
	// Layouter l;
	// l = new CurrentLayouter();
	// sb.append("***** " + l);
	// sb.append(ClassLayout.parseClass(klass, l).toPrintable());
	// for(FieldLayout fl : ClassLayout.parseClass(klass, l).fields()) {
	// if(fl.typeClass() == "org.tugraz.sysds.runtime.matrix.data.MatrixBlock") {
	// sb.append(ClassLayout.parseClass(MatrixBlock.class, l).toPrintable());
	// sb.append(ClassLayout.parseClass(DenseBlockFP64.class, l).toPrintable());
	// }
	// }
	// return sb.toString();
	// }

	private long getWorstCaseMemory(Class<?> klass) {
		l = new HotSpotLayouter(new X86_64_DataModel());
		long size = ClassLayout.parseClass(klass, l).instanceSize();

		for(FieldLayout fl : ClassLayout.parseClass(klass, l).fields()) {
			// If the type of filed is an Array, then add the cost of having such a thing.
			if(fl.typeClass().contains("[]")) {
				size += 20;
				size += 4;
			}
			if(fl.typeClass() == "org.tugraz.sysds.runtime.matrix.data.MatrixBlock") {
				size += MatrixBlock.estimateSizeDenseInMemory(0, 0);
			}
		}

		return size;
	}

}