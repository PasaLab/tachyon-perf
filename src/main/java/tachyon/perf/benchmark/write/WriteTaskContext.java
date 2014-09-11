package tachyon.perf.benchmark.write;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import tachyon.client.WriteType;
import tachyon.perf.basic.TaskContext;

/**
 * Record the statistics of write test.
 */
public class WriteTaskContext extends TaskContext {
  public static WriteTaskContext loadFromFile(File contextFile) throws IOException {
    WriteTaskContext ret = new WriteTaskContext();
    BufferedReader fin = new BufferedReader(new FileReader(contextFile));
    ret.mId = Integer.parseInt(fin.readLine());
    ret.mNodeName = fin.readLine();
    ret.mWriteType = WriteType.getOpType(fin.readLine());
    ret.mCores = Integer.parseInt(fin.readLine());
    ret.mTachyonWorkerBytes = Long.parseLong(fin.readLine());
    ret.mStartTimeMs = Long.parseLong(fin.readLine());
    ret.mFinishTimeMs = Long.parseLong(fin.readLine());
    ret.mSuccess = Boolean.parseBoolean(fin.readLine());
    int threadNum = Integer.parseInt(fin.readLine());
    if (threadNum >= 0) {
      ret.mThreadNum = threadNum;
      ret.mWriteBytes = new long[threadNum];
      ret.mWriteFiles = new int[threadNum];
      ret.mThreadTimeMs = new long[threadNum];
      for (int i = 0; i < threadNum; i++) {
        ret.mThreadTimeMs[i] = Long.parseLong(fin.readLine());
        ret.mWriteFiles[i] = Integer.parseInt(fin.readLine());
        ret.mWriteBytes[i] = Long.parseLong(fin.readLine());
      }
    }
    fin.close();
    return ret;
  }

  private int mCores;
  private long mTachyonWorkerBytes;

  private long[] mWriteBytes;
  private int[] mWriteFiles;
  private int mThreadNum = -1;
  private long[] mThreadTimeMs;
  private WriteType mWriteType;

  public int getCores() {
    return mCores;
  }

  public long getTachyonWorkerBytes() {
    return mTachyonWorkerBytes;
  }

  public int getThreadNum() {
    return mThreadNum;
  }

  public long[] getThreadTimeMs() {
    return mThreadTimeMs;
  }

  public WriteType getWriteType() {
    return mWriteType;
  }

  public long[] getWriteBytes() {
    return mWriteBytes;
  }

  public int[] getWriteFiles() {
    return mWriteFiles;
  }

  public void initial(WriteType writeType) throws IOException {
    mCores = Runtime.getRuntime().availableProcessors();
    mTachyonWorkerBytes = tachyon.conf.WorkerConf.get().MEMORY_SIZE;
    mWriteType = writeType;
  }

  public void setFromWriteThreads(WriteThread[] writeThreads) {
    mThreadNum = writeThreads.length;
    mWriteBytes = new long[mThreadNum];
    mWriteFiles = new int[mThreadNum];
    mThreadTimeMs = new long[mThreadNum];
    for (int i = 0; i < mThreadNum; i++) {
      WriteThreadStatistic statistics = writeThreads[i].getStatistic();
      mWriteBytes[i] = statistics.getSuccessBytes();
      mWriteFiles[i] = statistics.getSuccessFiles();
      mThreadTimeMs[i] = statistics.getFinishTimeMs() - statistics.getStartTimeMs();
      if (!statistics.getSuccess()) {
        mSuccess = false;
      }
    }
  }

  @Override
  public void writeToFile(String fileName) throws IOException {
    File contextFile = new File(fileName);
    BufferedWriter fout = new BufferedWriter(new FileWriter(contextFile));
    fout.write(mId + "\n");
    fout.write(mNodeName + "\n");
    fout.write(mWriteType.toString() + "\n");

    fout.write(mCores + "\n");
    fout.write(mTachyonWorkerBytes + "\n");

    fout.write(mStartTimeMs + "\n");
    fout.write(mFinishTimeMs + "\n");
    fout.write(mSuccess + "\n");
    fout.write(mThreadNum + "\n");
    if (mThreadNum >= 0) {
      for (int i = 0; i < mThreadNum; i++) {
        fout.write(mThreadTimeMs[i] + "\n");
        fout.write(mWriteFiles[i] + "\n");
        fout.write(mWriteBytes[i] + "\n");
      }
    }
    fout.close();
  }
}
