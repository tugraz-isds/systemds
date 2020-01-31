package org.tugraz.sysds.test.functions.io.json;

import org.apache.wink.json4j.JSONException;
import org.junit.Test;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.runtime.io.FrameReaderJSONL;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.runtime.util.DataConverter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FrameReaderJSONLTest {
    static final String FILENAME_SINGLE = "target/testTemp/functions/data/FrameJSONTest/out_random.json";

    @Test
    public void testReadFrameSingleFullFromHDFS() throws IOException, JSONException {
        FrameReaderJSONL frameReaderJSONL = new FrameReaderJSONL();

        Types.ValueType[] schema = new Types.ValueType[]{Types.ValueType.INT64, Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.INT64, Types.ValueType.INT64, Types.ValueType.INT64, Types.ValueType.INT64, Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.STRING};

        Map<String, Integer> schemaMap = new HashMap<String, Integer>();
        schemaMap.put("/id", 0); schemaMap.put("/title", 1); schemaMap.put("/authors", 2); schemaMap.put("/venue", 3); schemaMap.put("/venue/raw", 4); schemaMap.put("/year", 5);schemaMap.put("/n_citation", 6);schemaMap.put("/page_start", 7);schemaMap.put("/page_end", 8);schemaMap.put("/doc_type", 9);schemaMap.put("/publisher", 10);schemaMap.put("/volume", 11);schemaMap.put("/issue", 12);schemaMap.put("/fos", 13);


        FrameBlock input = frameReaderJSONL.readFrameFromHDFS(FILENAME_SINGLE, schema, schemaMap, 300, 4);
        //FrameBlock test = TestUtils.generateRandomFrameBlock(3, 5, 10);
        DataConverter.convertToStringFrame(input);

    }
}