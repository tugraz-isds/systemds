package org.tugraz.sysds.runtime.io;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
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
                                        String[] names, long rlen, long clen) throws IOException, DMLRuntimeException, JSONException {
        //prepare file access
        JobConf jobConf = new JobConf(ConfigurationManager.getCachedJobConf());
        Path path = new Path(fname);
        FileSystem fileSystem = IOUtilFunctions.getFileSystem(path, jobConf);
        FileInputFormat.addInputPath(jobConf, path);

        //check existence and non-empty file
        checkValidInputFile(fileSystem, path);


        Types.ValueType[] lschema = createOutputSchema(schema, clen);
        String[] lnames;
        if(names == null){
            lnames = createOutputNamesFromSchemaMap(schemaMap);
        } else {
            lnames = createOutputNames(names, clen);
        }
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

        for (int i=0, rowPos=0; i<splits.length; i++ ) {
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
            while (reader.next(key, value)) {
                // Potential Problem if JSON/L Object is very large
                JSONObject jsonObject = new JSONObject(value.toString());
                int col = 0;
                for (Map.Entry<String, Integer> entry : schemaMap.entrySet()) {
                    dest.set(row, col, UtilFunctions.stringToObject(schema[entry.getValue()],
                            jsonObject.get(entry.getKey()).toString()));
                    col++;
                }
                row++;
            }
        } finally {
            IOUtilFunctions.closeSilently(reader);
        }
        return row;
    }


    protected String[] createOutputNamesFromSchemaMap(Map<String, Integer> schemaMap){
        String[] names = new String[schemaMap.size()];
        schemaMap.forEach((key, value) -> {
            names[value] = key;
        });
        return names;
    }

    /*
    protected FrameBlock readJsonFrameFromHDFS(Path path, JobConf jobConf, Types.ValueType[] schema) throws IOException {

        TextInputFormat informat = new TextInputFormat();
        informat.configure(jobConf);
        InputSplit[] splits = informat.getSplits(jobConf, 1);

        LongWritable key = new LongWritable();
        Text value = new Text();

        int[] idims = Arrays.stream(dims).mapToInt(i -> (int) i).toArray();
        TensorBlock ret;
        if (schema.length == 1)
            ret = new TensorBlock(schema[0], idims).allocateBlock();
        else
            ret = new TensorBlock(schema, idims).allocateBlock();

        try {
            //int[] ix = new int[dims.length];
            for (InputSplit split : splits) {
                RecordReader<LongWritable, Text> reader = informat.getRecordReader(split, jobConf, Reporter.NULL);
                try {
                    while (reader.next(key, value)) {
                        JSONObject object = new JSONObject(value.toString());
                        System.out.println(object);
                    }
                } finally {
                    IOUtilFunctions.closeSilently(reader);
                }
            }
        } catch (Exception ex) {
            throw new IOException("Unable to read tensor in text cell format.", ex);
        }
        return null;


    }
     */



}
