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

import java.util.Arrays;

import org.tugraz.sysds.runtime.compress.colgroup.ColGroup;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupDDC1;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupOLE;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupRLE;
import org.tugraz.sysds.runtime.compress.colgroup.ColGroupUncompressed;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.DataConverter;

public class ConverterUtils {
	/**
	 * Copy col group instance with deep copy of column indices but shallow copy of actual contents;
	 * 
	 * @param group column group
	 * @return column group (deep copy of indices but shallow copy of contents)
	 */
	public static ColGroup copyColGroup(ColGroup group) {
		ColGroup ret = null;

		// deep copy col indices
		int[] colIndices = Arrays.copyOf(group.getColIndices(), group.getNumCols());

		// create copy of column group
		if(group instanceof ColGroupUncompressed) {
			ColGroupUncompressed in = (ColGroupUncompressed) group;
			ret = new ColGroupUncompressed(colIndices, in.getNumRows(), in.getData());
		}
		else if(group instanceof ColGroupRLE) {
			ColGroupRLE in = (ColGroupRLE) group;
			ret = new ColGroupRLE(colIndices, in.getNumRows(), in.hasZeros(), in.getValues(), in.getBitmaps(),
				in.getBitmapOffsets());
		}
		else if(group instanceof ColGroupOLE) {
			ColGroupOLE in = (ColGroupOLE) group;
			ret = new ColGroupOLE(colIndices, in.getNumRows(), in.hasZeros(), in.getValues(), in.getBitmaps(),
				in.getBitmapOffsets());
		}
		else if(group instanceof ColGroupDDC1) {
			ColGroupDDC1 in = (ColGroupDDC1) group;
			ret = new ColGroupDDC1(colIndices, in.getNumRows(), in.getValues(), in.getData());
		}
		else {
			throw new RuntimeException("Using '" + group.getClass() + "' instance of ColGroup not fully supported");
		}

		return ret;
	}

	public static double[] getDenseVector(MatrixBlock vector) {
		return DataConverter.convertToDoubleVector(vector, false);
	}

	public static MatrixBlock getUncompressedColBlock(ColGroup group) {
		return (group instanceof ColGroupUncompressed) ? ((ColGroupUncompressed) group)
			.getData() : new ColGroupUncompressed(Arrays.asList(group)).getData();
	}
}
