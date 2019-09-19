package org.tugraz.sysds.test.functions.builtin;

import org.junit.Test;
import org.junit.runners.Parameterized;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.lops.LopProperties;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

import java.util.*;

public class BuiltinL2SVMTest extends AutomatedTestBase {
    private final static String TEST_NAME = "l2svm";
    private final static String TEST_DIR = "functions/builtin/";
    private static final String TEST_CLASS_DIR = TEST_DIR + BuiltinL2SVMTest.class.getSimpleName() + "/";


    private final static double eps = 0.001;
    private final static int rows = 1000;
    private final static int colsX = 500;
    private final static int colsY = 1;
    private final static double spSparse = 0.01;
    private final static double spDense = 0.7;
    private final static int max_iter = 10;

    @Override
    public void setUp() {
        TestUtils.clearAssertionInformation();
        addTestConfiguration(TEST_NAME,new TestConfiguration(TEST_CLASS_DIR, TEST_NAME,new String[]{"C"}));
    }

    @Test
    public void testL2SVMDense() {
        runL2SVMTest(false, 0, eps, 1.0, max_iter, LopProperties.ExecType.CP);
    }
    @Test
    public void testL2SVMSparse() {
        runL2SVMTest(true, 0, eps, 1.0, max_iter, LopProperties.ExecType.CP);
    }
//
    @Test
    public void testL2SVMIntercept() {
        runL2SVMTest(true,1, eps, 1.0, max_iter, LopProperties.ExecType.SPARK);
    }
//
    @Test
    public void testL2SVMDenseIntercept() {
         runL2SVMTest(false,1, 1, 1.0, max_iter, LopProperties.ExecType.CP);
    }

    @Test
    public void testL2SVMSparseLambda2() {
        runL2SVMTest(true,1, 1, 2.0, max_iter, LopProperties.ExecType.CP);
    }

    @Test
    public void testL2SVMSparseLambda100CP() {
        runL2SVMTest(true,1, 1, 100, max_iter, LopProperties.ExecType.CP);
    }
    @Test
    public void testL2SVMSparseLambda100Spark() {
        runL2SVMTest(true,1, 1, 100, max_iter, LopProperties.ExecType.SPARK);
    }

    @Test
    public void testL2SVMIteration() {
        runL2SVMTest(true,1, 1, 2.0, 100, LopProperties.ExecType.CP);
    }

    private void runL2SVMTest(boolean sparse, int intercept, double eps,
                               double lambda, int run, LopProperties.ExecType instType)
    {
        Types.ExecMode platformOld = setExecMode(instType);

        boolean oldFlag = OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION;
        boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;

        try
        {
            loadTestConfiguration(getTestConfiguration(TEST_NAME));

            double sparsity = sparse ? spSparse : spDense;

            String HOME = SCRIPT_DIR + TEST_DIR;

            fullDMLScriptName = HOME + TEST_NAME + ".dml";
            programArgs = new String[]{ "-explain", "-stats",
                    "-nvargs", "X=" + input("X"), "Y=" + input("Y"), "model=" + output("model"),
                    "inc=" + intercept,"eps=" + eps, "lam=" + lambda, "max=" + run};


            fullRScriptName = HOME + TEST_NAME + ".R";
            rCmd = getRCmd(inputDir(), Integer.toString(intercept), Double.toString(eps),
                    Double.toString(lambda), Integer.toString(run), expectedDir());

            //generate actual datasets
            double[][] X = getRandomMatrix(rows, colsX, 0, 100, sparsity, 10);
            double[][] Y = getRandomMatrix(rows, 1, -1, 1, 1, -1);
            for(int i=0; i<rows; i++)
                Y[i][0] = (Y[i][0] > 0) ? 1 : -1;

            writeInputMatrixWithMTD("X", X, true);
            writeInputMatrixWithMTD("Y", Y, true);

            runTest(true, false, null, -1);
            runRScript(true);

            HashMap<MatrixValue.CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("model");
            HashMap<MatrixValue.CellIndex, Double> rfile  = readRMatrixFromFS("model");
            TestUtils.compareMatrices(dmlfile, rfile, eps, "Stat-DML", "Stat-R");

        }
        finally {
            rtplatform = platformOld;
            DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
            OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = oldFlag;
            OptimizerUtils.ALLOW_AUTO_VECTORIZATION = true;
            OptimizerUtils.ALLOW_OPERATOR_FUSION = true;
        }
    }
}
