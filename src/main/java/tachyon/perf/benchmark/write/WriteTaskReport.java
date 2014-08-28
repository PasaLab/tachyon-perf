package tachyon.perf.benchmark.write;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import tachyon.client.WriteType;
import tachyon.perf.basic.TaskReport;

/**
 * Record the statistics of write test.
 */
public class WriteTaskReport extends TaskReport {
  public static WriteTaskReport loadFromFile(File reportFile) throws IOException {
    BufferedReader fin = new BufferedReader(new FileReader(reportFile));
    String nodeName = fin.readLine();
    String writeType = fin.readLine();
    WriteTaskReport ret = new WriteTaskReport(nodeName, writeType);
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
      for (int i = 0; i < threadNum; i ++) {
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
  private int mThreadNum;
  private long[] mThreadTimeMs;
  private WriteType mWriteType;

  public WriteTaskReport(String nodeName, String writeType) throws IOException {
    super(nodeName);
    mCores = Runtime.getRuntime().availableProcessors();
    mTachyonWorkerBytes = tachyon.conf.WorkerConf.get().MEMORY_SIZE;
    mThreadNum = -1;
    mWriteType = WriteType.getOpType(writeType);
  }

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

  public void setFromWriteThreads(WriteThread[] writeThreads) {
    mThreadNum = writeThreads.length;
    mWriteBytes = new long[mThreadNum];
    mWriteFiles = new int[mThreadNum];
    mThreadTimeMs = new long[mThreadNum];
    for (int i = 0; i < mThreadNum; i ++) {
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
    File reportFile = new File(fileName);
    BufferedWriter fout = new BufferedWriter(new FileWriter(reportFile));
    fout.write(NODE_NAME + "\n");
    fout.write(mWriteType.toString() + "\n");

    fout.write(mCores + "\n");
    fout.write(mTachyonWorkerBytes + "\n");

    fout.write(mStartTimeMs + "\n");
    fout.write(mFinishTimeMs + "\n");
    fout.write(mSuccess + "\n");
    fout.write(mThreadNum + "\n");
    if (mThreadNum >= 0) {
      for (int i = 0; i < mThreadNum; i ++) {
        fout.write(mThreadTimeMs[i] + "\n");
        fout.write(mWriteFiles[i] + "\n");
        fout.write(mWriteBytes[i] + "\n");
      }
    }
    fout.close();
  }
}
