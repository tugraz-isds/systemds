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

package org.tugraz.sysds.test.functions.misc;

import org.junit.Test;
import org.junit.Assert;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;

public class RemoveUnnecessaryCTableTest extends AutomatedTestBase 
{
    private static final String TEST_NAME1 = "RewriteRemoveUnnecessaryCTable1L";
    private static final String TEST_NAME2 = "RewriteRemoveUnnecessaryCTable1R";
    private static final String TEST_NAME3 = "RewriteRemoveUnnecessaryCTableSameA";
    private static final String TEST_DIR = "functions/misc/";
	private static final String TEST_CLASS_DIR = TEST_DIR + RewriteEliminateRemoveEmptyTest.class.getSimpleName() + "/";
    private static final double[][] A = {{1},{2},{3},{4},{5},{6},{1},{2},{3},{4},{5},{6}};

    @Override
    public void setUp() 
    {
        TestUtils.clearAssertionInformation();
        addTestConfiguration( TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] { "s" }) );
        addTestConfiguration( TEST_NAME2, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2, new String[] { "s" }) );
        addTestConfiguration( TEST_NAME3, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME3, new String[] { "s" }) );
    }

    @Test
	public void testRemoveCTable1L() {
        double[][] sum = {{12}};
		testRewriteRemoveUnnecessaryCTable(TEST_NAME1, A, sum, true);
    }

    @Test
	public void testRemoveCTable1R() {
        double[][] sum = {{12}};
		testRewriteRemoveUnnecessaryCTable(TEST_NAME2, A, sum, false);
    }

    @Test
	public void testRemoveCTableSameA() {
        double[][] sum = {{12}};
		testRewriteRemoveUnnecessaryCTable(TEST_NAME2, A, sum, true);
    }

    @Test
	public void testRemoveCTableB() {
        double[][] B = {{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12}};
        double[][] sum = {{12}};
		testRewriteRemoveUnnecessaryCTable(TEST_NAME2, A, B, sum, true);
    }

    private void testRewriteRemoveUnnecessaryCTable(String test, double[][] A, double[][] B, double[][] sum, boolean checkHeavyHitters){
        boolean oldFlag = OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION;
        try
		{
			TestConfiguration config = getTestConfiguration(test);
			loadTestConfiguration(config);
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + test + ".dml";
			programArgs = new String[]{ "-explain", "-stats",
                "-args", input("A"), output(config.getOutputFiles()[0]) };
            
            OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = true;
                
            //give input
            writeInputMatrixWithMTD("A", A, true);
            writeInputMatrixWithMTD("B", B, true);
            
            //run test
            runTest(true, false, null, -1);
            
            //compare scalar
            double s = readDMLScalarFromHDFS("s").get(new CellIndex(1, 1));
            TestUtils.compareScalars(s,sum[0][0],1e-10);

            if( checkHeavyHitters )
				{
					boolean table = heavyHittersContainsSubString("table");
					Assert.assertFalse(table);
				}
		
		} 
		finally {
			OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = oldFlag;
		}
    }
    
    private void testRewriteRemoveUnnecessaryCTable(String test, double[][] A, double[][] sum, boolean checkHeavyHitters) {
        testRewriteRemoveUnnecessaryCTable(test, A, A, sum, checkHeavyHitters);
    }
}