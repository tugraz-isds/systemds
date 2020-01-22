package org.tugraz.sysds.runtime.io;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;
import org.tugraz.sysds.conf.ConfigurationManager;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.runtime.util.HDFSTool;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.Map;

public class FrameWriterJSONL{

    private static final int BLOCKSIZE = 320;

    public void writeFrameToHDFS(FrameBlock src, String fname, Map<String, Integer> schemaMap, long rlen, long clen)
            throws IOException, DMLRuntimeException, JSONException {

        //prepare file access
        JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());
        Path path = new Path( fname );

        //if the file already exists on HDFS, remove it.
        HDFSTool.deleteFileIfExistOnHDFS( fname );

        //validity check frame dimensions
        if( src.getNumRows() != rlen || src.getNumColumns() != clen ) {
            throw new IOException("Frame dimensions mismatch with metadata: " +
                    src.getNumRows()+"x"+src.getNumColumns()+" vs "+rlen+"x"+clen+".");
        }

        //core write (sequential/parallel)
        writeJSONLFrameToHDFS(path, job, src, rlen, clen, schemaMap);
    }

    protected void writeJSONLFrameToHDFS(Path path, JobConf jobConf, FrameBlock src, long rlen, long clen,
                                         Map<String, Integer> schemaMap) throws IOException, JSONException {
        FileSystem fileSystem = IOUtilFunctions.getFileSystem(path, jobConf);

        //sequential write to single text file
        writeJSONLFrameToFile(path, fileSystem, src, 0, (int)rlen, schemaMap);
        IOUtilFunctions.deleteCrcFilesFromLocalFileSystem(fileSystem, path);
    }

    protected void writeJSONLFrameToFile(Path path, FileSystem fileSystem, FrameBlock src, int lowerRowBound,
                                         int upperRowBound, Map<String, Integer> schemaMap) throws IOException, JSONException {

        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileSystem.create(path, true)));
        try {
            Iterator<String[]> stringRowIterator = src.getStringRowIterator(lowerRowBound, upperRowBound);
            StringBuilder stringBuilder = new StringBuilder();
            while (stringRowIterator.hasNext()) {
                String[] row = stringRowIterator.next();
                bufferedWriter.write(formatToJSONString(row, schemaMap) + "\n");
            }
        }finally {
            IOUtilFunctions.closeSilently(bufferedWriter);
        }
    }

    protected String formatToJSONString(String[] values, Map<String, Integer> schemaMap) throws IOException, JSONException {
        if(schemaMap.size() != values.length){
            throw new IOException("Schema Map and row mismatch. Cannot map " + values.length + " values to " +
                    schemaMap.size() + " JSON Objects");
        }
        JSONObject jsonObject = new JSONObject();

        for (Map.Entry<String, Integer> entry : schemaMap.entrySet()) {
            String key = entry.getKey();
            Integer value = entry.getValue();
            jsonObject.put(key, values[value]);
        }
        return jsonObject.write();
    }

}
