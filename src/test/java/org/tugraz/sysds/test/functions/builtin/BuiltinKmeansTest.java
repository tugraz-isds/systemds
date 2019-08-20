package org.tugraz.sysds.test.functions.builtin;

import org.junit.Assert;
import org.junit.Test;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.lops.LopProperties;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

import java.io.File;

public class BuiltinKmeansTest extends AutomatedTestBase
{
    private final static String TEST_NAME = "kmeans";
    private final static String TEST_DIR = "functions/builtin/";
    private static final String TEST_CLASS_DIR = TEST_DIR + BuiltinKmeansTest.class.getSimpleName() + "/";
    private final static String TEST_CONF_DEFAULT = "SystemML-config-codegen.xml";
    private final static File TEST_CONF_FILE_DEFAULT = new File(SCRIPT_DIR + TEST_DIR, TEST_CONF_DEFAULT);
    private final static String TEST_CONF_FUSE_ALL = "SystemML-config-codegen-fuse-all.xml";
    private final static File TEST_CONF_FILE_FUSE_ALL = new File(SCRIPT_DIR + TEST_DIR, TEST_CONF_FUSE_ALL);
    private final static String TEST_CONF_FUSE_NO_REDUNDANCY = "SystemML-config-codegen-fuse-no-redundancy.xml";
    private final static File TEST_CONF_FILE_FUSE_NO_REDUNDANCY = new File(SCRIPT_DIR + TEST_DIR,
            TEST_CONF_FUSE_NO_REDUNDANCY);
    private enum TestType { DEFAULT,FUSE_ALL,FUSE_NO_REDUNDANCY }
    private final static double eps = 1e-10;
    private final static int rows = 3972;
    private final static int cols = 972;
    private final static double spSparse = 0.3;
    private final static double spDense = 0.7;
    private final static double max_iter = 10;

    private TestType currentTestType = TestType.DEFAULT;

    @Override
    public void setUp() {
        TestUtils.clearAssertionInformation();
        addTestConfiguration(TEST_NAME,new TestConfiguration(TEST_CLASS_DIR, TEST_NAME,new String[]{"C"}));
    }

    @Test
    public void testKMeansDenseBinSingleRewritesCP() {
        runKMeansTest(false, 2, 1, true, LopProperties.ExecType.CP, TestType.DEFAULT);
    }

    @Test
    public void testKMeansSparseBinSingleRewritesCP() {
        runKMeansTest(true,2, 1, true,  LopProperties.ExecType.CP,  TestType.DEFAULT);
    }

    @Test
    public void testKMeansDenseBinSingleCP() {
        runKMeansTest(false,2, 1, false,  LopProperties.ExecType.CP,  TestType.DEFAULT);
    }

    @Test
    public void testKMeansSparseBinSingleCP() {
        runKMeansTest(true, 2, 1, false, LopProperties.ExecType.CP, TestType.DEFAULT);
    }

    @Test
    public void testKMeansDenseBinMultiRewritesCP() {
        runKMeansTest(false, 2, 10, true, LopProperties.ExecType.CP, TestType.DEFAULT);
    }

    @Test
    public void testKMeansSparseBinMultiRewritesCP() {
        runKMeansTest(true, 2, 10, true, LopProperties.ExecType.CP, TestType.DEFAULT);
    }

    @Test
    public void testKMeansDenseBinMultiCP() {
        runKMeansTest(false, 2, 10, false, LopProperties.ExecType.CP, TestType.DEFAULT);
    }

    @Test
    public void testKMeansSparseBinMultiCP() {
        runKMeansTest(true, 2, 10, false, LopProperties.ExecType.CP, TestType.DEFAULT);
    }

    @Test
    public void testKMeansDenseMulSingleRewritesCP() {
        runKMeansTest(false, 20, 1, true, LopProperties.ExecType.CP, TestType.DEFAULT);
    }

    @Test
    public void testKMeansSparseMulSingleRewritesCP() {
        runKMeansTest(true, 20, 1, true, LopProperties.ExecType.CP, TestType.DEFAULT);
    }

    @Test
    public void testKMeansDenseMulSingleCP() {
        runKMeansTest(false, 20, 1, false, LopProperties.ExecType.CP, TestType.DEFAULT);
    }

    @Test
    public void testKMeansSparseMulSingleCP() {
        runKMeansTest(true, 20, 1, false, LopProperties.ExecType.CP, TestType.DEFAULT);
    }

    @Test
    public void testKMeansDenseMulMultiRewritesCP() {
        runKMeansTest( false, 20, 10, true, LopProperties.ExecType.CP, TestType.DEFAULT);
    }

    @Test
    public void testKMeansSparseMulMultiRewritesCP() {
        runKMeansTest(true, 20, 10, true, LopProperties.ExecType.CP, TestType.DEFAULT);
    }

    @Test
    public void testKMeansDenseMulMultiCP() {
        runKMeansTest(false, 20, 10, false, LopProperties.ExecType.CP, TestType.DEFAULT);
    }

    @Test
    public void testKMeansSparseMulMultiCP() {
        runKMeansTest(true, 20, 10, false, LopProperties.ExecType.CP, TestType.DEFAULT);
    }

    @Test
    public void testKMeansDenseBinSingleRewritesCPFuseAll() {
        runKMeansTest(false, 2, 1, true, LopProperties.ExecType.CP, TestType.FUSE_ALL);
    }

    @Test
    public void testKMeansSparseBinSingleRewritesCPFuseAll() {
        runKMeansTest(true, 2, 1, true, LopProperties.ExecType.CP, TestType.FUSE_ALL);
    }

    @Test
    public void testKMeansDenseBinSingleCPFuseAll() {
        runKMeansTest(false, 2, 1, false, LopProperties.ExecType.CP, TestType.FUSE_ALL);
    }

    @Test
    public void testKMeansSparseBinSingleCPFuseAll() {
        runKMeansTest(true, 2, 1, false, LopProperties.ExecType.CP, TestType.FUSE_ALL);
    }

    @Test
    public void testKMeansDenseBinMultiRewritesCPFuseAll() {
        runKMeansTest(false, 2, 10, true, LopProperties.ExecType.CP, TestType.FUSE_ALL);
    }

    @Test
    public void testKMeansSparseBinMultiRewritesCPFuseAll() {
        runKMeansTest(true, 2, 10, true, LopProperties.ExecType.CP, TestType.FUSE_ALL);
    }

    @Test
    public void testKMeansDenseBinMultiCPFuseAll() {
        runKMeansTest(false, 2, 10, false, LopProperties.ExecType.CP, TestType.FUSE_ALL);
    }

    @Test
    public void testKMeansSparseBinMultiCPFuseAll() {
        runKMeansTest( true, 2, 10, false, LopProperties.ExecType.CP, TestType.FUSE_ALL);
    }

    @Test
    public void testKMeansDenseMulSingleRewritesCPFuseAll() {
        runKMeansTest(false, 20, 1, true, LopProperties.ExecType.CP, TestType.FUSE_ALL);
    }

    @Test
    public void testKMeansSparseMulSingleRewritesCPFuseAll() {
        runKMeansTest(true, 20, 1, true, LopProperties.ExecType.CP, TestType.FUSE_ALL);
    }

    @Test
    public void testKMeansDenseMulSingleCPFuseAll() {
        runKMeansTest( false, 20, 1, false, LopProperties.ExecType.CP, TestType.FUSE_ALL);
    }

    @Test
    public void testKMeansSparseMulSingleCPFuseAll() {
        runKMeansTest(true, 20, 1, false, LopProperties.ExecType.CP, TestType.FUSE_ALL);
    }

    @Test
    public void testKMeansDenseMulMultiRewritesCPFuseAll() {
        runKMeansTest(false, 20, 10, true, LopProperties.ExecType.CP, TestType.FUSE_ALL);
    }

    @Test
    public void testKMeansSparseMulMultiRewritesCPFuseAll() {
        runKMeansTest(true, 20, 10, true, LopProperties.ExecType.CP, TestType.FUSE_ALL);
    }

    @Test
    public void testKMeansDenseMulMultiCPFuseAll() {
        runKMeansTest(false, 20, 10, false, LopProperties.ExecType.CP, TestType.FUSE_ALL);
    }

    @Test
    public void testKMeansSparseMulMultiCPFuseAll() {
        runKMeansTest(true, 20, 10, false, LopProperties.ExecType.CP, TestType.FUSE_ALL);
    }

    @Test
    public void testKMeansDenseBinSingleRewritesCPFuseNoRedundancy() {
        runKMeansTest(false, 2, 1, true, LopProperties.ExecType.CP,
                TestType.FUSE_NO_REDUNDANCY);
    }

    @Test
    public void testKMeansSparseBinSingleRewritesCPFuseNoRedundancy() {
        runKMeansTest(true, 2, 1, true, LopProperties.ExecType.CP,
                TestType.FUSE_NO_REDUNDANCY);
    }

    @Test
    public void testKMeansDenseBinSingleCPFuseNoRedundancy() {
        runKMeansTest(false, 2, 1, false, LopProperties.ExecType.CP,
                TestType.FUSE_NO_REDUNDANCY);
    }

    @Test
    public void testKMeansSparseBinSingleCPFuseNoRedundancy() {
        runKMeansTest(true, 2, 1, false, LopProperties.ExecType.CP,
                TestType.FUSE_NO_REDUNDANCY);
    }

    @Test
    public void testKMeansDenseBinMultiRewritesCPFuseNoRedundancy() {
        runKMeansTest(false, 2, 10, true, LopProperties.ExecType.CP,
                TestType.FUSE_NO_REDUNDANCY);
    }

    @Test
    public void testKMeansSparseBinMultiRewritesCPFuseNoRedundancy() {
        runKMeansTest(true, 2, 10, true, LopProperties.ExecType.CP,
                TestType.FUSE_NO_REDUNDANCY);
    }

    @Test
    public void testKMeansDenseBinMultiCPFuseNoRedundancy() {
        runKMeansTest(false, 2, 10, false, LopProperties.ExecType.CP,
                TestType.FUSE_NO_REDUNDANCY);
    }

    @Test
    public void testKMeansSparseBinMultiCPFuseNoRedundancy() {
        runKMeansTest(true, 2, 10, false, LopProperties.ExecType.CP,
                TestType.FUSE_NO_REDUNDANCY);
    }

    @Test
    public void testKMeansDenseMulSingleRewritesCPFuseNoRedundancy() {
        runKMeansTest(false, 20, 1, true, LopProperties.ExecType.CP,
                TestType.FUSE_NO_REDUNDANCY);
    }

    @Test
    public void testKMeansSparseMulSingleRewritesCPFuseNoRedundancy() {
        runKMeansTest(true, 20, 1, true, LopProperties.ExecType.CP,
                TestType.FUSE_NO_REDUNDANCY);
    }

    @Test
    public void testKMeansDenseMulSingleCPFuseNoRedundancy() {
        runKMeansTest( false, 20, 1, false, LopProperties.ExecType.CP,
                TestType.FUSE_NO_REDUNDANCY);
    }

    @Test
    public void testKMeansSparseMulSingleCPFuseNoRedundancy() {
        runKMeansTest( true, 20, 1, false, LopProperties.ExecType.CP,
                TestType.FUSE_NO_REDUNDANCY);
    }

    @Test
    public void testKMeansDenseMulMultiRewritesCPFuseNoRedundancy() {
        runKMeansTest(false, 20, 10, true, LopProperties.ExecType.CP,
                TestType.FUSE_NO_REDUNDANCY);
    }

    @Test
    public void testKMeansSparseMulMultiRewritesCPFuseNoRedundancy() {
        runKMeansTest(true, 20, 10, true, LopProperties.ExecType.CP,
                TestType.FUSE_NO_REDUNDANCY);
    }

    @Test
    public void testKMeansDenseMulMultiCPFuseNoRedundancy() {
        runKMeansTest(false, 20, 10, false, LopProperties.ExecType.CP,
                TestType.FUSE_NO_REDUNDANCY);
    }

    @Test
    public void testKMeansSparseMulMultiCPFuseNoRedundancy() {
        runKMeansTest(true,20, 10, false, LopProperties.ExecType.CP,
                TestType.FUSE_NO_REDUNDANCY);
    }

    // added last two test cases as SPARK to have some and get rid of a warning
    @Test
    public void testKMeansDenseMulMultiSparkFuseNoRedundancy() {
        runKMeansTest(false, 20, 10, false, LopProperties.ExecType.SPARK,
                TestType.FUSE_NO_REDUNDANCY);
    }

    @Test
    public void testKMeansSparseMulMultiSparkFuseNoRedundancy() {
        runKMeansTest(true,20, 10, false, LopProperties.ExecType.SPARK,
                TestType.FUSE_NO_REDUNDANCY);
    }

    private void runKMeansTest(boolean sparse, int centroids, int runs, boolean rewrites,
                               LopProperties.ExecType instType, TestType testType)
    {
        Types.ExecMode platformOld = setExecMode(instType);

        boolean oldFlag = OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION;
        currentTestType = testType;
        boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;

        try
        {
            loadTestConfiguration(getTestConfiguration(TEST_NAME));

            double sparsity = sparse ? spSparse : spDense;

            String HOME = SCRIPT_DIR + TEST_DIR;

            fullDMLScriptName = HOME + TEST_NAME + ".dml";
            programArgs = new String[]{ "-explain", "-stats",
                    "-nvargs", "X=" + input("X"), "Y=" + output("Y"), "C=" + output("C"),
                    "k=" + centroids, "runs=" + runs,
                    "eps=" + eps, "max_iter=" + max_iter};

            OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = rewrites;

            //generate actual datasets
            double[][] X = getRandomMatrix(rows, cols, 0, 1, sparsity, 714);
            writeInputMatrixWithMTD("X", X, true);


            runTest(true, false, null, -1);

            Assert.assertTrue(heavyHittersContainsSubString("spoofCell") || heavyHittersContainsSubString("sp_spoofCell"));
            Assert.assertTrue(heavyHittersContainsSubString("spoofRA") || heavyHittersContainsSubString("sp_spoofRA"));
        }
        finally {
            rtplatform = platformOld;
            DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
            OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = oldFlag;
            OptimizerUtils.ALLOW_AUTO_VECTORIZATION = true;
            OptimizerUtils.ALLOW_OPERATOR_FUSION = true;
        }
    }

    /**
     * Override default configuration with custom test configuration to ensure
     * scratch space and local temporary directory locations are also updated.
     */
    @Override
    protected File getConfigTemplateFile()
    {
        // Instrumentation in this test's output log to show custom configuration file used for template.
        String message = "This test case overrides default configuration with ";
        if(currentTestType == TestType.FUSE_ALL){
            System.out.println(message + TEST_CONF_FILE_FUSE_ALL.getPath());
            return TEST_CONF_FILE_FUSE_ALL;
        } else if(currentTestType == TestType.FUSE_NO_REDUNDANCY){
            System.out.println(message + TEST_CONF_FILE_FUSE_NO_REDUNDANCY.getPath());
            return TEST_CONF_FILE_FUSE_NO_REDUNDANCY;
        } else {
            System.out.println(message + TEST_CONF_FILE_DEFAULT.getPath());
            return TEST_CONF_FILE_DEFAULT;
        }
    }
}
