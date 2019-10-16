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

package org.tugraz.sysds.test.functions.unary.matrix;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class typeOfTest extends AutomatedTestBase
{
    private final static String TEST_NAME = "typeOf";
    private final static String TEST_DIR = "functions/unary/frame/";
    private static final String TEST_CLASS_DIR = TEST_DIR + typeOfTest.class.getSimpleName() + "/";
    private static final String DATAPATH = SCRIPT_DIR+"functions/unary/frame/input/";
    private static final String DATASET1 = "Adult_csv/adult2.csv";
    private static final String DATASET2 = "Adult_csv/adult.csv";

    private final static int rows = 120;
    private final static int cols = 5;

    private final static double sparsityDense = 0.7;
    private final static double sparsitySparse = 0.07;

    @Override
    public void setUp()
    {
        TestUtils.clearAssertionInformation();

        addTestConfiguration(TEST_NAME,
                new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] { "Y" }) );

        if (TEST_CACHE_ENABLED) {
            setOutAndExpectedDeletionDisabled(true);
        }
    }

    @BeforeClass
    public static void init() {
        TestUtils.clearDirectory(TEST_DATA_DIR + TEST_CLASS_DIR);
    }

    @AfterClass
    public static void cleanUp() {
        if (TEST_CACHE_ENABLED) {
            TestUtils.clearDirectory(TEST_DATA_DIR + TEST_CLASS_DIR);
        }
    }

    @Test
    public void testTypeOfCP() {
        runtypeOfTest( DATASET1, 120, 5, true, ExecType.CP  );
    }

    @Test
    public void testTypeOfSpark() {
        runtypeOfTest(DATASET1, 120, 5, true, ExecType.SPARK);
    }

    @Test
    public void testTypeOfCPD2() {
        runtypeOfTest( DATASET2, 32561, 15, false, ExecType.CP   );
    }

    @Test
    public void testTypeOfSparkD2() {
        runtypeOfTest(DATASET2, 32561, 15, false, ExecType.SPARK);
    }

    private void runtypeOfTest( String dataset, int rows, int cols, boolean header, ExecType et  )
    {

        boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
        if( et == ExecType.SPARK )
            DMLScript.USE_LOCAL_SPARK_CONFIG = true;
        try {
            getAndLoadTestConfiguration(TEST_NAME);

            /* This is for running the junit test the new way, i.e., construct the arguments directly */
            String HOME = SCRIPT_DIR + TEST_DIR;
            fullDMLScriptName = HOME + TEST_NAME + ".dml";
            programArgs = new String[]{"-explain", "recompile_runtime", "-args", DATAPATH+dataset, String.valueOf(rows), String.valueOf(cols), String.valueOf(header).toUpperCase(),  output("schema.csv")};

            runTest(true, false, null, -1);
        }
        catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}