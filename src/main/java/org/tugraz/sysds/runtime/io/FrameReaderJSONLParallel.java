package org.tugraz.sysds.runtime.io;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.runtime.util.CommonThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class FrameReaderJSONLParallel extends FrameReaderJSONL {


    @Override
    protected void readJSONLFrameFromHDFS(Path path, JobConf jobConf, FileSystem fileSystem, FrameBlock dest,
                                          Types.ValueType[] schema, Map<String, Integer> schemaMap)
            throws IOException{

        int numThreads = OptimizerUtils.getParallelTextReadParallelism();

        TextInputFormat inputFormat = new TextInputFormat();
        inputFormat.configure(jobConf);
        InputSplit[] splits = inputFormat.getSplits(jobConf, numThreads);
        splits = IOUtilFunctions.sortInputSplits(splits);

        try{
            ExecutorService executorPool = CommonThreadPool.get(Math.min(numThreads, splits.length));

            //compute num rows per split
            ArrayList<CountRowsTask> countRowsTasks = new ArrayList<>();
            for (InputSplit split : splits){
                countRowsTasks.add(new CountRowsTask(split, inputFormat, jobConf));
            }
            List<Future<Long>> futureCountList = executorPool.invokeAll(countRowsTasks);

            //compute row offset per split via cumsum on row counts
            long offset = 0;
            List<Long> offsets = new ArrayList<>();
            for( Future<Long> count : futureCountList ) {
                offsets.add(offset);
                offset += count.get();
            }

            //read individual splits
            ArrayList<ReadRowsTask> readRowsTasks = new ArrayList<>();
            for( int i=0; i<splits.length; i++ ) {
                readRowsTasks.add(new ReadRowsTask(splits[i], inputFormat, jobConf, dest, schemaMap, offsets.get(i).intValue()));
            }
            List<Future<Object>> rret = executorPool.invokeAll(readRowsTasks);
            executorPool.shutdown();

            //error handling
            for( Future<Object> read : rret )
                read.get();

        }catch (Exception e) {
            throw new IOException("Failed parallel read of JSONL input.", e);
        }

    }

    private static class CountRowsTask implements Callable<Long> {
        private InputSplit _split;
        private TextInputFormat _inputFormat;
        private JobConf _jobConf;

        public CountRowsTask(InputSplit split, TextInputFormat inputFormat, JobConf jobConf){
            _split = split;
            _inputFormat = inputFormat;
            _jobConf = jobConf;
        }

        @Override
        public Long call() throws Exception {
            RecordReader<LongWritable, Text> recordReader = _inputFormat.getRecordReader(_split, _jobConf, Reporter.NULL);
            LongWritable key = new LongWritable();
            Text value = new Text();
            long nrows = 0;

            try{
                while (recordReader.next(key, value)){
                    nrows++;
                }
            } finally {
                IOUtilFunctions.closeSilently(recordReader);
            }
            return nrows;
        }
    }


    private class ReadRowsTask implements Callable<Object>{
        private InputSplit _split;
        private TextInputFormat _inputFormat;
        private JobConf _jobConf;
        private FrameBlock _dest;
        Map<String, Integer> _schemaMap;
        private int _offset;

        public ReadRowsTask(InputSplit split, TextInputFormat inputFormat, JobConf jobConf,
                            FrameBlock dest, Map<String, Integer> schemaMap, int offset)
        {
            _split = split;
            _inputFormat = inputFormat;
            _jobConf = jobConf;
            _dest = dest;
            _schemaMap = schemaMap;
            _offset = offset;
        }


        @Override
        public Object call() throws Exception {
            readJSONLFrameFromInputSplit(_split, _inputFormat, _jobConf, _dest.getSchema(), _schemaMap, _dest, _offset);
            return null;
        }
    }
}
