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

package org.tugraz.sysds.test.functions.data;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Random;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

/**
 * Complete suit of tests for Rand:
 * - changing sparsity
 * - changing dimensions
 * - changing distribution
 * - changing the runtime platform
 */

@RunWith(value = Parameterized.class)
public class RandRuntimePlatformTest extends AutomatedTestBase 
{
	
	private final static String TEST_DIR = "functions/data/";
	private final static String TEST_NAME = "RandRuntimePlatformTest";
	private final static String TEST_CLASS_DIR = TEST_DIR + RandRuntimePlatformTest.class.getSimpleName() + "/";
	
	private final static double eps = 1e-10;
	
	private static final int _dim1=1, _dim2=500, _dim3=1000, _dim4=1001, _dim5=1500, _dim6=2500, _dim7=10000;
	private static final double _sp1=0.2, _sp2=0.4, _sp3=1.0, _sp4=1e-6;
	private static final long _seed = 1L;
	
	private int rows, cols;
	private double sparsity;
	private long seed;
	private String pdf;
	
	public RandRuntimePlatformTest(int r, int c, double sp, long sd, String dist) {
		rows = r;
		cols = c;
		sparsity = sp;
		seed = sd;
		pdf = dist;
	}
	
	@Parameters
	public static Collection<Object[]> data() {
		Object[][] data = new Object[][] { 
				// ---- Uniform distribution ----
				{_dim1, _dim1, _sp2, _seed, "uniform"},
				{_dim1, _dim1, _sp3, _seed, "uniform"},
				// vectors
				{_dim5, _dim1, _sp1, _seed, "uniform"}, 
				{_dim5, _dim1, _sp2, _seed, "uniform"}, 
				{_dim5, _dim1, _sp3, _seed, "uniform"},
				// single block data
				{_dim3, _dim2, _sp1, _seed, "uniform"}, 
				{_dim3, _dim2, _sp2, _seed, "uniform"}, 
				{_dim3, _dim2, _sp3, _seed, "uniform"},
				
				{_dim3, _dim3, _sp1, _seed, "uniform"}, 
				{_dim3, _dim3, _sp2, _seed, "uniform"}, 
				{_dim3, _dim3, _sp3, _seed, "uniform"},
				// multi-block data
				{_dim4, _dim4, _sp1, _seed, "uniform"},
				{_dim4, _dim4, _sp2, _seed, "uniform"},
				{_dim4, _dim4, _sp3, _seed, "uniform"},
				
				{_dim4, _dim6, _sp1, _seed, "uniform"},
				{_dim4, _dim6, _sp2, _seed, "uniform"},
				{_dim4, _dim6, _sp3, _seed, "uniform"},
				
				{_dim6, _dim4, _sp1, _seed, "uniform"},
				{_dim6, _dim4, _sp2, _seed, "uniform"},
				{_dim6, _dim4, _sp3, _seed, "uniform"},
				
				{_dim6, _dim6, _sp1, _seed, "uniform"},
				{_dim6, _dim6, _sp2, _seed, "uniform"},
				{_dim6, _dim6, _sp3, _seed, "uniform"},
				
				// Ultra-sparse data
				{_dim7, _dim7, _sp4, _seed, "uniform"},

				// ---- Normal distribution ----
				{_dim1, _dim1, _sp2, _seed, "normal"},
				{_dim1, _dim1, _sp3, _seed, "normal"},
				// vectors
				{_dim5, _dim1, _sp1, _seed, "normal"}, 
				{_dim5, _dim1, _sp2, _seed, "normal"}, 
				{_dim5, _dim1, _sp3, _seed, "normal"},
				// single block data
				{_dim3, _dim2, _sp1, _seed, "normal"}, 
				{_dim3, _dim2, _sp2, _seed, "normal"}, 
				{_dim3, _dim2, _sp3, _seed, "normal"}, 
				{_dim3, _dim3, _sp1, _seed, "normal"}, 
				{_dim3, _dim3, _sp2, _seed, "normal"}, 
				{_dim3, _dim3, _sp3, _seed, "normal"},
				// multi-block data
				{_dim4, _dim4, _sp1, _seed, "normal"},
				{_dim4, _dim4, _sp2, _seed, "normal"},
				{_dim4, _dim4, _sp3, _seed, "normal"},
				{_dim4, _dim6, _sp1, _seed, "normal"},
				{_dim4, _dim6, _sp2, _seed, "normal"},
				{_dim4, _dim6, _sp3, _seed, "normal"},
				{_dim6, _dim4, _sp1, _seed, "normal"},
				{_dim6, _dim4, _sp2, _seed, "normal"},
				{_dim6, _dim4, _sp3, _seed, "normal"},
				{_dim6, _dim6, _sp1, _seed, "normal"},
				{_dim6, _dim6, _sp2, _seed, "normal"},
				{_dim6, _dim6, _sp3, _seed, "normal"},
				
				// ---- Poisson distribution ----
				{_dim1, _dim1, _sp2, _seed, "poisson"},
				{_dim1, _dim1, _sp3, _seed, "poisson"},
				// vectors
				{_dim5, _dim1, _sp1, _seed, "poisson"}, 
				{_dim5, _dim1, _sp2, _seed, "poisson"}, 
				{_dim5, _dim1, _sp3, _seed, "poisson"},
				// single block data
				{_dim3, _dim2, _sp1, _seed, "poisson"}, 
				{_dim3, _dim2, _sp2, _seed, "poisson"}, 
				{_dim3, _dim2, _sp3, _seed, "poisson"}, 
				{_dim3, _dim3, _sp1, _seed, "poisson"}, 
				{_dim3, _dim3, _sp2, _seed, "poisson"}, 
				{_dim3, _dim3, _sp3, _seed, "poisson"},
				// multi-block data
				{_dim4, _dim4, _sp1, _seed, "poisson"},
				{_dim4, _dim4, _sp2, _seed, "poisson"},
				{_dim4, _dim4, _sp3, _seed, "poisson"},
				{_dim4, _dim6, _sp1, _seed, "poisson"},
				{_dim4, _dim6, _sp2, _seed, "poisson"},
				{_dim4, _dim6, _sp3, _seed, "poisson"},
				{_dim6, _dim4, _sp1, _seed, "poisson"},
				{_dim6, _dim4, _sp2, _seed, "poisson"},
				{_dim6, _dim4, _sp3, _seed, "poisson"},
				{_dim6, _dim6, _sp1, _seed, "poisson"},
				{_dim6, _dim6, _sp2, _seed, "poisson"},
				{_dim6, _dim6, _sp3, _seed, "poisson"},
				
				// Ultra-sparse data
				{_dim7, _dim7, _sp4, _seed, "uniform"}
				
				};
		return Arrays.asList(data);
	}
	
	@Override
	public void setUp() 
	{
		addTestConfiguration(TEST_NAME,new TestConfiguration(TEST_CLASS_DIR, TEST_NAME,new String[]{"A"})); 
	}
	
	@Test
	public void testRandAcrossRuntimePlatforms()
	{
		ExecMode platformOld = rtplatform;
	
		try
		{
			getAndLoadTestConfiguration(TEST_NAME);
			
			/* This is for running the junit test the new way, i.e., construct the arguments directly */
			String HOME = SCRIPT_DIR + TEST_DIR;
			
			if ( !pdf.equalsIgnoreCase("poisson"))
			{
				fullDMLScriptName = HOME + TEST_NAME + ".dml";
				programArgs = new String[]{"-args", 
					Integer.toString(rows), Integer.toString(cols),
					Double.toString(sparsity), Long.toString(seed), pdf, 
					output("A_CP") };
			}
			else 
			{
				Random r = new Random(System.nanoTime());
				double mean = r.nextDouble()*100;
				fullDMLScriptName = HOME + TEST_NAME + "Poisson" + ".dml";
				programArgs = new String[]{"-args", 
					Integer.toString(rows), Integer.toString(cols),
					Double.toString(sparsity), Long.toString(seed), pdf, Double.toString(mean), 
					output("A_CP") };
			}
	
			boolean exceptionExpected = false;
			
			// Generate Data in CP
			rtplatform = ExecMode.SINGLE_NODE;
			programArgs[programArgs.length-1] = output("A_SN"); // data file generated from CP
			runTest(true, exceptionExpected, null, -1); 
						
			
			// Generate Data in CP
			rtplatform = ExecMode.HYBRID;
			programArgs[programArgs.length-1] = output("A_CP"); // data file generated from CP
			runTest(true, exceptionExpected, null, -1); 
			
			boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
			try {
				// Generate Data in Spark
				rtplatform = ExecMode.SPARK;
				DMLScript.USE_LOCAL_SPARK_CONFIG = true;
				programArgs[programArgs.length-1] = output("A_SPARK"); // data file generated from MR
				runTest(true, exceptionExpected, null, -1); 
			}
			finally {
				DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
			}
		
			//compare matrices (1-2, 2-3 -> transitively 1-3)
			HashMap<CellIndex, Double> cpfile = readDMLMatrixFromHDFS("A_CP");
			HashMap<CellIndex, Double> spfile = readDMLMatrixFromHDFS("A_SPARK");
			TestUtils.compareMatrices(spfile, cpfile, eps, "SPFile", "CPFile");
			
		}
		finally
		{
			rtplatform = platformOld;
		}
	}
}
