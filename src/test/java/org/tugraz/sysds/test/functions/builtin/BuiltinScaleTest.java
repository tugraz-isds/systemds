package org.tugraz.sysds.test.integration.functions.builtin;

import java.util.HashMap;

import org.junit.Test;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class BuiltinScaleTest extends AutomatedTestBase 
{
	private final static String TEST_NAME = "Scale";
	private final static String TEST_DIR = "functions/builtin/";
	private static final String TEST_CLASS_DIR = TEST_DIR + BuiltinScaleTest.class.getSimpleName() + "/";
	
	private final static double eps = 1e-8;
	private final static int rows = 1765;
	private final static int cols = 392;
	private final static double spSparse = 0.7;
	private final static double spDense = 0.1;
	
	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME,new TestConfiguration(TEST_CLASS_DIR, TEST_NAME,new String[]{"B"})); 
	}

	@Test
	public void testScaleDenseNegNegCP() {
		runScaleTest(false, false, false, ExecType.CP);
	}
	
	@Test
	public void testScaleDenseNegPosCP() {
		runScaleTest(false, false, true, ExecType.CP);
	}
	
	@Test
	public void testScaleDensePosNegCP() {
		runScaleTest(false, true, false, ExecType.CP);
	}
	
	@Test
	public void testScaleDensePosPosCP() {
		runScaleTest(false, true, true, ExecType.CP);
	}
	
	@Test
	public void testScaleDenseNegNegSP() {
		runScaleTest(false, false, false, ExecType.SPARK);
	}
	
	@Test
	public void testScaleDenseNegPosSP() {
		runScaleTest(false, false, true, ExecType.SPARK);
	}
	
	@Test
	public void testScaleDensePosNegSP() {
		runScaleTest(false, true, false, ExecType.SPARK);
	}
	
	@Test
	public void testScaleDensePosPosSP() {
		runScaleTest(false, true, true, ExecType.SPARK);
	}
	
	@Test
	public void testScaleSparseNegNegCP() {
		runScaleTest(true, false, false, ExecType.CP);
	}
	
	@Test
	public void testScaleSparseNegPosCP() {
		runScaleTest(true, false, true, ExecType.CP);
	}
	
	@Test
	public void testScaleSparsePosNegCP() {
		runScaleTest(true, true, false, ExecType.CP);
	}
	
	@Test
	public void testScaleSparsePosPosCP() {
		runScaleTest(true, true, true, ExecType.CP);
	}
	
	@Test
	public void testScaleSparseNegNegSP() {
		runScaleTest(true, false, false, ExecType.SPARK);
	}
	
	@Test
	public void testScaleSparseNegPosSP() {
		runScaleTest(true, false, true, ExecType.SPARK);
	}
	
	@Test
	public void testScaleSparsePosNegSP() {
		runScaleTest(true, true, false, ExecType.SPARK);
	}
	
	@Test
	public void testScaleSparsePosPosSP() {
		runScaleTest(true, true, true, ExecType.SPARK);
	}
	
	private void runScaleTest(boolean sparse, boolean center, boolean scale, ExecType instType)
	{
		ExecMode platformOld = rtplatform;
		switch( instType ) {
			case SPARK: rtplatform = ExecMode.SPARK; break;
			default: rtplatform = ExecMode.HYBRID; break;
		}
		
		try
		{
			loadTestConfiguration(getTestConfiguration(TEST_NAME));
			
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{"-explain", "-args", input("A"),
				String.valueOf(center).toUpperCase(), String.valueOf(scale).toUpperCase(), 
				output("B") };
			fullRScriptName = HOME + TEST_NAME + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir() + " " 
				+ String.valueOf(center).toUpperCase() + " " + String.valueOf(scale).toUpperCase() + 
				" " + expectedDir();
			
			//generate actual dataset 
			double[][] A = getRandomMatrix(rows, cols, -1, 1, sparse?spSparse:spDense, 7);
			writeInputMatrixWithMTD("A", A, true);
			
			runTest(true, false, null, -1);
			runRScript(true);
			
			//compare matrices
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("B");
			HashMap<CellIndex, Double> rfile  = readRMatrixFromFS("B");
			TestUtils.compareMatrices(dmlfile, rfile, eps, "Stat-DML", "Stat-R");
		}
		finally {
			rtplatform = platformOld;
		}
	}

}
