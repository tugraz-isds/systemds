package org.tugraz.sysds.test.functions.io.json;

import org.apache.wink.json4j.JSONException;
import org.junit.Test;
import org.tugraz.sysds.runtime.io.FrameWriterJSONL;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.test.TestUtils;

import java.io.IOException;
import java.util.Map;

public class FrameWriterJSONLTest {
    static final String FILENAME_OUTPUT = "target/testTemp/functions/data/FrameJSONTest/out_random.json";

    @Test
    public void testWriteFrameToHDFS() throws IOException, JSONException {
        FrameWriterJSONL frameWriterJSONL = new FrameWriterJSONL();
        int rows = 30;
        int cols = 40;
        Map<String, Integer> schemaMap = TestUtils.generateRandomSchemaMap(cols, 100);
        FrameBlock input = TestUtils.generateRandomFrameBlock(rows, cols, -1);


        //double[][] input = TestUtils.generateTestMatrix(rows, cols, -10, 10, 0.9, 93874);
        //FrameBlock fInput = DataConverter.convertToFrameBlock(DataConverter.convertToMatrixBlock(input));

        frameWriterJSONL.writeFrameToHDFS(input, FILENAME_OUTPUT, schemaMap, rows,cols);
        //performanceTest(frameWriterJSONL, schemaMap, input, 2);
    }

    private void performanceTest(FrameWriterJSONL frameWriterJSONL, Map<String, Integer> schemaMap, FrameBlock input, int iterations) throws IOException, JSONException {
        long sum = 0;
        for(int i = 0; i < iterations; i++){
            long startTime = System.nanoTime();
            frameWriterJSONL.writeFrameToHDFS(input,FILENAME_OUTPUT, schemaMap, 3000,400);
            long endTime = System.nanoTime();
            sum += (endTime - startTime);
            System.out.println((endTime - startTime) / 1000000 + "ms");
        }

        System.out.println((sum/iterations) / 1000000 + "ms");
    }
}