package tachyon.perf.benchmark.skipRead;

import java.io.IOException;
import java.util.List;

import tachyon.perf.basic.PerfThread;
import tachyon.perf.basic.TaskConfiguration;
import tachyon.perf.benchmark.ListGenerator;
import tachyon.perf.benchmark.Operators;
import tachyon.perf.conf.PerfConf;
import tachyon.perf.fs.PerfFileSystem;

public class SkipReadThread extends PerfThread {
  protected int mBufferSize;
  protected PerfFileSystem mFileSystem;
  protected long mReadBytes;
  protected List<String> mReadFiles;
  protected String mReadType;
  protected long mSkipBytes;
  protected boolean mSkipOnce;

  protected boolean mSuccess;
  protected double mThroughput; // in MB/s

  public boolean getSuccess() {
    return mSuccess;
  }

  public double getThroughput() {
    return mThroughput;
  }

  @Override
  public void run() {
    long timeMs = System.currentTimeMillis();
    long readBytes = 0;
    mSuccess = true;
    for (String fileName : mReadFiles) {
      try {
        if (mSkipOnce) {
          readBytes +=
              Operators.skipReadOnce(mFileSystem, fileName, mBufferSize, mSkipBytes, mReadBytes,
                  mReadType);
        } else {
          readBytes +=
              Operators.skipReadToEnd(mFileSystem, fileName, mBufferSize, mSkipBytes, mReadBytes,
                  mReadType);
        }
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
    mSkipBytes = taskConf.getLongProperty("skip.bytes");
    mSkipOnce = taskConf.getBooleanProperty("skip.and.read.once");
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

  @Override
  public boolean cleanupThread(TaskConfiguration taskConf) {
    try {
      Operators.close(mFileSystem);
    } catch (IOException e) {
      LOG.warn("Error when close file system, task " + mTaskId + " - thread " + mId, e);
    }
    return true;
  }
}
