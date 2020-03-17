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

package org.tugraz.sysds.test;

/**
 * Class Containing Testing Constants, for easy enumeration of typical Parameters classes
 */
public class TestConstants {

	private static final int rows[] = {4, 2008, 1283, 5, 1, 251, 5000, 100000, 3123};
	private static final int cols[] = {20, 20, 13, 998, 321, 1, 30, 21, 1};
	private static final double[] sparsityValues = {0.9, 0.1, 0.01, 0.0};

	private static final int[] mins = {-10, -2147};
	private static final int[] maxs = {10, 2147};

	public enum SparsityType {
		DENSE, SPARSE, ULTRA_SPARSE, EMPTY,
	}

	public enum ValueType {
		RAND, // UC
		CONST, // RLE
		RAND_ROUND_OLE, // OLE
		RAND_ROUND_DDC, // RLE
	}

	public enum MatrixType {
		SMALL, // A Small Matrix
		LARGE, // A "Large" Matrix
		FEW_ROW, // A Matrix with a large number of rows and less columns
		FEW_COL, // A Matrix with a large number of columns
		SINGLE_ROW, // Single Row with some columns
		SINGLE_COL, // Single Column with some rows
		L_ROWS, XL_ROWS, SINGLE_COL_L, // Single Column large.
	}

	public enum ValueRange {
		SMALL, LARGE
	}

	public enum CompressionType {
		LOSSLESS, LOSSY
	}

	public static double getSparsityValue(SparsityType sparsityType) {
		switch(sparsityType) {
			case DENSE:
				return sparsityValues[0];
			case SPARSE:
				return sparsityValues[1];
			case ULTRA_SPARSE:
				return sparsityValues[2];
			case EMPTY:
				return sparsityValues[3];
			default:
				throw new RuntimeException("Invalid Sparsity type"); 
		}
	}

	public static int getMinRangeValue(ValueRange valueRange) {
		switch(valueRange) {
			case SMALL:
				return mins[0];
			case LARGE:
				return mins[1];
			default:
			throw new RuntimeException("Invalid range value enum type"); 
		}
	}

	public static int getMaxRangeValue(ValueRange valueRange) {
		switch(valueRange) {
			case SMALL:
				return maxs[0];
			case LARGE:
				return maxs[1];
			default:
				throw new RuntimeException("Invalid range value enum type"); 
		}
	}

	public static int getNumberOfRows(MatrixType matrixType) {
		switch(matrixType) {
			case SMALL:
				return rows[0];
			case LARGE:
				return rows[1];
			case FEW_ROW:
				return rows[2];
			case FEW_COL:
				return rows[3];
			case SINGLE_ROW:
				return rows[4];
			case SINGLE_COL:
				return rows[5];
			case L_ROWS:
				return rows[6];
			case XL_ROWS:
				return rows[7];
			case SINGLE_COL_L:
				return rows[8];
			default:
				throw new RuntimeException("Invalid matrix enum type"); 
		}
	}

	public static int getNumberOfColumns(MatrixType matrixType) {
		switch(matrixType) {
			case SMALL:
				return cols[0];
			case LARGE:
				return cols[1];
			case FEW_ROW:
				return cols[2];
			case FEW_COL:
				return cols[3];
			case SINGLE_ROW:
				return cols[4];
			case SINGLE_COL:
				return cols[5];
			case L_ROWS:
				return cols[6];
			case XL_ROWS:
				return cols[7];
			case SINGLE_COL_L:
				return cols[8];
			default:
				throw new RuntimeException("Invalid matrix enum type"); 
		}
	}
}
