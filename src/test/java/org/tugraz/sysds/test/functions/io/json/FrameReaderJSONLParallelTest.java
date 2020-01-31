package org.tugraz.sysds.test.functions.io.json;

import org.apache.wink.json4j.JSONException;
import org.junit.Test;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.runtime.io.FrameReaderJSONLParallel;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FrameReaderJSONLParallelTest {
    static final String FILENAME = "target/testTemp/functions/data/FrameJSONTest/in_1000000.json";
    static final String FILENAME_HUGE = "target/testTemp/functions/data/FrameJSONTest/dblp_papers_v11.txt";

    @Test
    public void testReadFrameFromHDFS() throws IOException, JSONException {
        FrameReaderJSONLParallel frameReaderJSONL = new FrameReaderJSONLParallel();

        Types.ValueType[] schema = new Types.ValueType[]{Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.STRING, Types.ValueType.STRING};

        Map<String, Integer> schemaMap = new HashMap<String, Integer>();
        schemaMap.put("/id", 0); schemaMap.put("/title", 1); schemaMap.put("/authors", 2); schemaMap.put("/venue", 3); schemaMap.put("/venue/raw", 4); schemaMap.put("/year", 5);schemaMap.put("/n_citation", 6);schemaMap.put("/page_start", 7);schemaMap.put("/page_end", 8);schemaMap.put("/doc_type", 9);schemaMap.put("/publisher", 10);schemaMap.put("/volume", 11);schemaMap.put("/issue", 12);schemaMap.put("/fos", 13);



        FrameBlock input = frameReaderJSONL.readFrameFromHDFS(FILENAME, schema, schemaMap, 5000000,13);
        input.getNumColumns();
    }
}