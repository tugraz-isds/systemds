package org.tugraz.sysds.test.functions.io.json;

import org.apache.wink.json4j.JSONException;
import org.junit.Test;
import org.tugraz.sysds.runtime.io.FrameWriterJSONL;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.test.TestUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class FrameWriterJSONLTest {
    static final String FILENAME_OUTPUT = "target/testTemp/functions/data/FrameJSONTest/out_random.json";

    @Test
    public void testWriteFrameToHDFS() throws IOException, JSONException {
        FrameWriterJSONL frameWriterJSONL = new FrameWriterJSONL();
        int rows = 3000;
        int cols = 3000;
        Map<String, Integer> schemaMap = generateRandomSchemaMap(cols);
        /*
        Map<String, Integer> schemaMap = new HashMap<String, Integer>();
        schemaMap.put("id", 0);
        schemaMap.put("title", 1);
        schemaMap.put("authors", 2);

         */

        double[][] input = TestUtils.generateTestMatrix(rows, cols, -10, 10, 0.9, 93874);
        FrameBlock fInput = DataConverter.convertToFrameBlock(DataConverter.convertToMatrixBlock(input));

        frameWriterJSONL.writeFrameToHDFS(fInput, FILENAME_OUTPUT, schemaMap, rows,cols);

    }


    private Map<String, Integer> generateRandomSchemaMap(int cols){
        Map<String, Integer> schemaMap = new HashMap<String, Integer>();
        for(int i = 0; i < cols; i++){
            byte[] array = new byte[7]; // length is bounded by 7
            new Random().nextBytes(array);
            String generatedString = new Random().ints('a', 'z' + 1)
                    .limit(10)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();
            schemaMap.put(generatedString, i);
        }
        return schemaMap;
    }

    private void performanceTest(FrameWriterJSONL frameWriterJSONL, Map<String, Integer> schemaMap, FrameBlock input, int iterations) throws IOException, JSONException {
        long sum = 0;
        for(int i = 0; i < iterations; i++){
            long startTime = System.nanoTime();
            frameWriterJSONL.writeFrameToHDFS(input,FILENAME_OUTPUT, schemaMap, 500,3);
            long endTime = System.nanoTime();
            sum += (endTime - startTime);
            System.out.println((endTime - startTime) / 1000000 + "ms");
        }

        System.out.println((sum/iterations) / 1000000 + "ms");
    }
}