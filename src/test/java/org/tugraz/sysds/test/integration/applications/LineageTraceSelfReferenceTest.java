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

package org.tugraz.sysds.test.integration.applications;

import org.junit.runners.Parameterized.Parameters;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestUtils;

import java.util.*;

public abstract class LineageTraceSelfReferenceTest extends AutomatedTestBase {

    protected final static String TEST_DIR = "applications/lineage_trace/";
    protected final static String TEST_NAME = "LineageTraceSelfReference";
    protected String TEST_CLASS_DIR = TEST_DIR + LineageTraceSelfReferenceTest.class.getSimpleName() + "/";

    protected int numRecords, numFeatures;

    public LineageTraceSelfReferenceTest(int rows, int cols) {
        numRecords = rows;
        numFeatures = cols;
    }

    @Parameters
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][]{
                {10, 5} //, {1000, 500}
        };
        return Arrays.asList(data);
    }

    @Override
    public void setUp() {
        addTestConfiguration(TEST_CLASS_DIR, TEST_NAME);
    }

    protected void testLineageTrace() {
        System.out.println("------------ BEGIN " + TEST_NAME + "------------");

        int rows = numRecords;
        int cols = numFeatures;

        getAndLoadTestConfiguration(TEST_NAME);

//        rtplatform = Types.ExecMode.SPARK;
//        DMLScript.USE_LOCAL_SPARK_CONFIG = true;

        List<String> proArgs = new ArrayList<String>();

        proArgs.add("-stats");
//        proArgs.add("-explain");
//        proArgs.add("hops");
        proArgs.add("-lineage");
        proArgs.add("-args");
        proArgs.add(input("X"));
        proArgs.add(output("X"));
        proArgs.add(output("Y"));
        programArgs = proArgs.toArray(new String[proArgs.size()]);

        fullDMLScriptName = getScript();

        double[][] X = getRandomMatrix(rows, cols, 0, 1, 0.8, -1);
        writeInputMatrixWithMTD("X", X, true);

        String expected_X_lineage =
                "(1) target/testTemp/applications/lineage_trace/LineageTraceSelfReferenceDMLTest/in/X\n" +
                "(2) false\n" +
                "(3) createvar (1) (2)\n" +
                "(7) rblk (3)\n" +
                "(10) cpvar (7)\n" +
                "(20) 7\n" +
                "(21) + (10) (20)\n" +
                "(22) cpvar (21)\n" +
                "(35) * (22) (22)\n" +
                "(39) * (35) (22)\n" +
                "(40) cpvar (39)\n" +
                "(53) 7\n" +
                "(54) + (40) (53)\n" +
                "(55) cpvar (54)\n";

        String expected_Y_lineage = expected_X_lineage + "(64) tsmm (55)\n";


        /*
         * Expected number of jobs:
         * Rand - 1 job
         * Computation before while loop - 4 jobs
         * While loop iteration - 10 jobs
         * Final output write - 1 job
         */
        int expectedNumberOfJobs = 16;
        runTest(true, EXCEPTION_NOT_EXPECTED, null, expectedNumberOfJobs);

        HashMap<CellIndex, Double> X_DML = readDMLMatrixFromHDFS("X");
        HashMap<CellIndex, Double> Y_DML = readDMLMatrixFromHDFS("Y");

        String X_lineage = readDMLLineageFromHDFS("X");
        String Y_lineage = readDMLLineageFromHDFS("Y");

//        HINT:
//        Disable rewriting rule RewriteMatrixMultChainOptimization ()
//        and simplifyBinaryToUnaryOperation ()

        TestUtils.compareScalars(expected_X_lineage, X_lineage);
        TestUtils.compareScalars(expected_Y_lineage, Y_lineage);


    }
}
