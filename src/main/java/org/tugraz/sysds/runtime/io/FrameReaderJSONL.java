package org.tugraz.sysds.runtime.io;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.conf.ConfigurationManager;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.runtime.util.UtilFunctions;

import java.io.IOException;
import java.util.Map;

import static org.tugraz.sysds.runtime.io.FrameReader.*;


public class FrameReaderJSONL {
    public FrameBlock readFrameFromHDFS(String fname, Types.ValueType[] schema, Map<String, Integer> schemaMap,
                                        long rlen, long clen) throws IOException, DMLRuntimeException, JSONException {
        //prepare file access
        JobConf jobConf = new JobConf(ConfigurationManager.getCachedJobConf());
        Path path = new Path(fname);
        FileSystem fileSystem = IOUtilFunctions.getFileSystem(path, jobConf);
        FileInputFormat.addInputPath(jobConf, path);

        //check existence and non-empty file
        checkValidInputFile(fileSystem, path);


        Types.ValueType[] lschema = createOutputSchema(schema, clen);
        String[] lnames = createOutputNamesFromSchemaMap(schemaMap);
        FrameBlock ret = createOutputFrameBlock(lschema, lnames, rlen);

        readJSONLFrameFromHDFS(path, jobConf, fileSystem, ret, schema, schemaMap);
        return ret;
        //return readJsonFrameFromHDFS(path, jobConf, schema);
    }


    protected void readJSONLFrameFromHDFS(Path path, JobConf jobConf, FileSystem fileSystem, FrameBlock dest,
                                          Types.ValueType[] schema, Map<String, Integer> schemaMap) throws IOException, JSONException {
        TextInputFormat inputFormat = new TextInputFormat();
        inputFormat.configure(jobConf);
        InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
        splits = IOUtilFunctions.sortInputSplits(splits);

        for (int i = 0, rowPos = 0; i < splits.length; i++) {
            rowPos = readJSONLFrameFromInputSplit(splits[i], inputFormat, jobConf, schema, schemaMap, dest, rowPos);
        }
    }


    protected final int readJSONLFrameFromInputSplit(InputSplit split, InputFormat<LongWritable, Text> inputFormat,
                                                     JobConf jobConf, Types.ValueType[] schema,
                                                     Map<String, Integer> schemaMap, FrameBlock dest, int currentRow)
            throws IOException, JSONException {
        RecordReader<LongWritable, Text> reader = inputFormat.getRecordReader(split, jobConf, Reporter.NULL);
        LongWritable key = new LongWritable();
        Text value = new Text();

        int row = currentRow;
        try {
            //ObjectMapper mapper = new ObjectMapper();
            while (reader.next(key, value)) {
                // Potential Problem if JSON/L Object is very large
                JSONObject jsonObject = new JSONObject(value.toString());

                //JsonNode root = mapper.readTree(value.toString());

                for (Map.Entry<String, Integer> entry : schemaMap.entrySet()) {

/*
                    String strCellValue;
                    JsonNode cellValue = root.at(entry.getKey());
                    if (cellValue.isArray()) {
                        strCellValue = cellValue.toString();
                    } else {
                        strCellValue = cellValue.asText();
                    }



 */
                    String strCellValue = getStringFromJSONPath(jsonObject, entry.getKey());


                    //String strCellValue = jsonObject.get(entry.getKey()).toString();


                    dest.set(row, entry.getValue(), UtilFunctions.stringToObject(schema[entry.getValue()], strCellValue));
                }
                row++;
            }
        } finally {
            IOUtilFunctions.closeSilently(reader);
        }
        return row;
    }

    private String getStringFromJSONPath(JSONObject jsonObject, String path) throws IOException {
        String[] splitPath = path.split("/");
        Object temp = null;
        for (String split : splitPath) {
            if(split.equals("")){ continue; }
            try{
                if (temp == null) {
                    temp = jsonObject.get(split);
                } else if (temp instanceof JSONObject) {
                    temp = ((JSONObject) temp).get(split);
                } else if (temp instanceof JSONArray) {
                    throw new IOException("Cannot traverse JSON Array in a meaningful manner");
                } else {
                    return null;
                }
            } catch (JSONException e){
                // Value not in JsonObject
                return null;
            }

        }
        if(temp == null){
            throw new IOException("Could not traverse the JSON path: '" + path + "'!");
        }
        return temp.toString();
    }


    private String[] createOutputNamesFromSchemaMap(Map<String, Integer> schemaMap) {
        String[] names = new String[schemaMap.size()];
        schemaMap.forEach((key, value) -> names[value] = key);
        return names;
    }

}
