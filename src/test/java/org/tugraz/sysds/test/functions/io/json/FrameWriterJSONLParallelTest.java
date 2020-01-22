package org.tugraz.sysds.test.functions.io.json;

import org.apache.wink.json4j.JSONException;
import org.junit.Test;
import org.tugraz.sysds.runtime.io.FrameWriterJSONLParallel;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.test.TestUtils;

import java.io.IOException;
import java.util.Map;

public class FrameWriterJSONLParallelTest {

    static final String FILENAME_OUTPUT = "target/testTemp/functions/data/FrameJSONTest/out_random.json";

    @Test
    public void testWriteFrameToHDFS() throws IOException, JSONException {
        FrameWriterJSONLParallel frameWriterJSONL = new FrameWriterJSONLParallel();
        int rows = 3000;
        int cols = 3000;
        Map<String, Integer> schemaMap = FrameWriterJSONLTest.generateRandomSchemaMap(cols);
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
}