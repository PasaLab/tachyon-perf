package tachyon.perf.benchmark.skipRead;

import java.io.IOException;
import java.util.List;
import tachyon.perf.basic.TaskConfiguration;
import tachyon.perf.benchmark.ListGenerator;
import tachyon.perf.benchmark.Operators;
import tachyon.perf.conf.PerfConf;

public class SkipRead2Thread extends SkipReadThread {
  private int mSkipTimes;

  @Override
  public void run() {
    long readBytes = 0;
    mSuccess = true;
    long timeMs = System.currentTimeMillis();
    for (String fileName : mReadFiles) {
      try {
        readBytes +=
            Operators.randomSkipRead(mFileSystem, fileName, mBufferSize, mReadBytes, mReadType,
                mSkipTimes);
      } catch (IOException e) {
        LOG.error("Failed to read file " + fileName, e);
        mSuccess = false;
      }
    }
    timeMs = System.currentTimeMillis() - timeMs;
    mThroughput = (readBytes / 1024.0 / 1024.0) / (timeMs / 1000.0);
  }

  @Override
  public boolean setupThread(TaskConfiguration taskConf) {
    mBufferSize = taskConf.getIntProperty("buffer.size.bytes");
    mReadBytes = taskConf.getLongProperty("read.bytes");
    mSkipTimes = taskConf.getIntProperty("skip.times.per.file");
    mReadType = taskConf.getProperty("read.type");
    try {
      mFileSystem = Operators.connect((PerfConf.get().FS_ADDRESS));
      String readDir = taskConf.getProperty("read.dir");
      List<String> candidates = mFileSystem.listFullPath(readDir);
      if (candidates == null || candidates.isEmpty()) {
        throw new IOException("No file to read");
      }
      boolean isRandom = "RANDOM".equals(taskConf.getProperty("read.mode"));
      int filesNum = taskConf.getIntProperty("files.per.thread");
      if (isRandom) {
        mReadFiles = ListGenerator.generateRandomReadFiles(filesNum, candidates);
      } else {
        mReadFiles =
            ListGenerator.generateSequenceReadFiles(mId, PerfConf.get().THREADS_NUM, filesNum,
                candidates);
      }
    } catch (IOException e) {
      LOG.error("Failed to setup thread, task " + mTaskId + " - thread " + mId, e);
      return false;
    }
    mSuccess = false;
    mThroughput = 0;
    return true;
  }
}