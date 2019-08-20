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

package org.tugraz.sysds.test.functions.builtin;


import org.junit.Test;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;

//package io;
import java.util.*;

public class BuiltinSliceFinderTest extends AutomatedTestBase {

	private final static String TEST_NAME = "slicefinder";
	private final static String TEST_DIR = "functions/builtin/";
	private static final String TEST_CLASS_DIR = TEST_DIR + BuiltinSliceFinderTest.class.getSimpleName() + "/";

	private final static double eps = 1e-10;
	private final static int rows = 32000;
	private final static int cols = 10;
	private final static double spSparse = 1;
	private final static double spDense = 10;


	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[]{"B"}));
	}

	@Test
	public void SingleFreatureTest() { runslicefindertest(1,true, ExecType.CP, BuiltinLmTest.LinregType.AUTO); }


	@Test
	public void MultipleValuesOneFeature() { runslicefindertest(2,true, ExecType.CP, BuiltinLmTest.LinregType.AUTO); }

	@Test
	public void MultipleFeaturesSingleValues() { runslicefindertest(3,true, ExecType.CP, BuiltinLmTest.LinregType.AUTO); }


	public double[][] RandomizeArray(double[][]y){
		int i;
		Random rgen=new Random();

		for(i=0;i<y.length;i++){
			int randomPosition=rgen.nextInt(y.length);
			double temp=y[i][0];
			y[i][0]=y[randomPosition][0];
			y[randomPosition][0]=temp;

		}

		return y;

	}

	public double[][] modifyvalue(double[][]A,double[][]Y, int value, int coll){

		Integer i, j;

		int counter = 0;
		double nvec[][] = new double[rows][1];
		for (i = 0; i < rows; i++) {
			if (A[i][coll] == value) {
				nvec[counter][0] = Y[i][0];
				counter++;
			}
		}
		double[][] y = new double[counter][1];
		for (i = 0; i < counter; i++) {
			y[i][0] = nvec[i][0];

		}

		double[][] yy = RandomizeArray(y);
		int dim = cols + 1;
		double AA [][] = new double[rows][dim];
		counter = 0;

		for(i = 0;i<rows;i++){
			for(j = 0; j < dim;j++){

				if(j == cols ){
					AA[i][j] = Y[i][0];
				}else{
					AA[i][j] = A[i][j];
				}
			}

			if(A[i][coll] == value){  // this condition changes the values you choose

				AA[i][10] = yy[counter][0];
				counter++;
			}
		}
		return AA;

	}

	private void runslicefindertest(int test,boolean sparse, ExecType instType, BuiltinLmTest.LinregType linregAlgo) {

		Integer i, j;
		ExecMode platformOld = setExecMode(instType);
		String dml_test_name = TEST_NAME;
		loadTestConfiguration(getTestConfiguration(TEST_NAME));
		String HOME = SCRIPT_DIR + TEST_DIR;


		try {
			loadTestConfiguration(getTestConfiguration(TEST_NAME));
			double sparsity = sparse ? spSparse : spDense;
			fullDMLScriptName = HOME + dml_test_name + ".dml";
			programArgs = new String[]{"-explain", "-args", input("AA"), input("B")};
			double[][] A = getRandomMatrix(rows, cols, 0, 10, 1, 7);
			double[][] B = getRandomMatrix(10, 1, 0, 10, 1.0, 3);
			double[][] As = new double[rows][cols];
			double [][] Ys = new double[rows][1];

			for (i = 0; i < rows; i++) {
				for (j = 0; j < cols; j++) {

					//A[i][j] = A[i][j];
					A[i][j] = Math.ceil(A[i][j]);
					if (i == 0) {
						B[j][0] = Math.ceil(B[j][0]);
					}
				}
			}

			double Y[][] = new double[rows][1];
			for (i = 0; i < rows; i++) {
				for (j = 0; j < 1; j++) {
					for (int k = 0; k < cols; k++) {

						Y[i][j] += A[i][k] * B[k][j];
					}
				}
			}
			/*for (i = 0; i < rows; i++) {
				for (j = 0; j < cols; j++) {

					System.out.print(Y[i][0] + " ");
				}
				System.out.println();
			}
*/


			double AA[][] = new double[rows][cols+1];



			if(test == 1){

				 AA = modifyvalue(A, Y,7,5);


			}else if(test == 2) {

				 AA = modifyvalue(A, Y, 6, 3);

				for(i = 0;i<rows;i++){
					for(j = 0; j < cols+1;j++){

						if(j == cols ){
							Ys[i][0] = (int) AA[i][j];
						}else{
							As[i][j] = AA[i][j];
						}
					}
				}

				 AA = modifyvalue(As,Ys,3,3);

			}else{

				AA = modifyvalue(A, Y, 6, 3);

				for(i = 0;i<rows;i++){
					for(j = 0; j < cols+1;j++){

						if(j == cols ){
							Ys[i][0] = (int) AA[i][j];
						}else{
							As[i][j] = AA[i][j];
						}
					}
				}

				AA = modifyvalue(As,Ys,3,7);
			}

			writeInputMatrixWithMTD("AA", AA, true);
			writeInputMatrixWithMTD("B", B, true);
			//writeInputMatrixWithMTD("A", A, true);


			runTest(true, false, null, -1);

		} finally {
			rtplatform = platformOld;
		}

	}

}

