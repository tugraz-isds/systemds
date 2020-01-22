package org.tugraz.sysds.runtime.io;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.wink.json4j.JSONException;
import org.tugraz.sysds.conf.DMLConfig;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.runtime.matrix.data.OutputInfo;
import org.tugraz.sysds.runtime.util.CommonThreadPool;
import org.tugraz.sysds.runtime.util.HDFSTool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class FrameWriterJSONLParallel extends FrameWriterJSONL {
    @Override
    protected void writeJSONLFrameToHDFS(Path path, JobConf jobConf, FrameBlock src, long rlen, long clen,
                                         Map<String, Integer> schemaMap) throws IOException, JSONException {

        //estimate output size and number of output blocks (min 1)
        int numPartFiles = Math.max((int)(OptimizerUtils.estimateSizeTextOutput(rlen, clen, rlen*clen,
                OutputInfo.stringToOutputInfo("JSONL"))  / InfrastructureAnalyzer.getHDFSBlockSize()), 1);

        //determine degree of parallelism
        int numThreads = OptimizerUtils.getParallelTextWriteParallelism();
        numThreads = Math.min(numThreads, numPartFiles);

        //fall back to sequential write if dop is 1 (e.g., <128MB) in order to create single file
        if( numThreads <= 1 ) {
            super.writeJSONLFrameToHDFS(path, jobConf, src, rlen, clen, schemaMap);
            return;
        }

        //create directory for concurrent tasks
        HDFSTool.createDirIfNotExistOnHDFS(path, DMLConfig.DEFAULT_SHARED_DIR_PERMISSION);
        FileSystem fileSystem = IOUtilFunctions.getFileSystem(path, jobConf);

        //create and execute tasks
        try
        {
            ExecutorService pool = CommonThreadPool.get(numThreads);
            ArrayList<WriteFileTask> tasks = new ArrayList<>();
            int blklen = (int)Math.ceil((double)rlen / numThreads);
            for(int i=0; i<numThreads & i*blklen<rlen; i++) {
                Path newPath = new Path(path, IOUtilFunctions.getPartFileName(i));
                tasks.add(new WriteFileTask(newPath, fileSystem, src, i*blklen, (int)Math.min((i+1)*blklen, rlen), schemaMap));
            }

            //wait until all tasks have been executed
            List<Future<Object>> rt = pool.invokeAll(tasks);
            pool.shutdown();

            //check for exceptions
            for( Future<Object> task : rt )
                task.get();

            // delete crc files if written to local file system
            if (fileSystem instanceof LocalFileSystem) {
                for(int i=0; i<numThreads & i*blklen<rlen; i++)
                    IOUtilFunctions.deleteCrcFilesFromLocalFileSystem(fileSystem,
                            new Path(path, IOUtilFunctions.getPartFileName(i)));
            }
        }
        catch (Exception e) {
            throw new IOException("Failed parallel write of JSONL output.", e);
        }
    }

    private class WriteFileTask implements Callable<Object>{

        private Path _path;
        private FileSystem _fileSystem;
        private FrameBlock _src;
        private int _lowerRowBound;
        private int _upperRowBound;
        Map<String, Integer> _schemaMap;

        public WriteFileTask(Path _path,  FileSystem _fileSystem, FrameBlock _src, int _lowerRowBound,
                             int _upperRowBound, Map<String, Integer> _schemaMap) {
            this._path = _path;
            this._fileSystem = _fileSystem;
            this._src = _src;
            this._lowerRowBound = _lowerRowBound;
            this._upperRowBound = _upperRowBound;
            this._schemaMap = _schemaMap;
        }

        @Override
        public Object call() throws Exception {
            writeJSONLFrameToFile(_path, _fileSystem, _src, _lowerRowBound, _upperRowBound, _schemaMap );
            return null;
        }
    }

}
