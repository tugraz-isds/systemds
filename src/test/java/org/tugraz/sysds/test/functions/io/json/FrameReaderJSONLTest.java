package org.tugraz.sysds.test.functions.io.json;

import org.apache.wink.json4j.JSONException;
import org.junit.Test;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.runtime.io.FrameReaderJSONL;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FrameReaderJSONLTest {
    static final String FILENAME = "target/testTemp/functions/data/FrameJSONTest/in_50000.json";

    @Test
    public void testReadFrameFromHDFS() throws IOException, JSONException {
        FrameReaderJSONL frameReaderJSONL = new FrameReaderJSONL();

        Types.ValueType[] schema = new Types.ValueType[]{Types.ValueType.INT64, Types.ValueType.STRING, Types.ValueType.STRING};


        Map<String, Integer> schemaMap = new HashMap<String, Integer>();
        schemaMap.put("id", 0);
        schemaMap.put("title", 1);
        schemaMap.put("authors", 2);


        FrameBlock input = frameReaderJSONL.readFrameFromHDFS(FILENAME, schema, schemaMap, null, 500000,3);
        input.getNumColumns();
    }
}