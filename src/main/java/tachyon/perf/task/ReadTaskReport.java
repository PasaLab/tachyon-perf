package tachyon.perf.task;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import tachyon.client.ReadType;
import tachyon.perf.thread.PerfThread;
import tachyon.perf.thread.RWThreadReport;

public class ReadTaskReport extends TaskReport {
  public static ReadTaskReport loadFromFile(File reportFile) throws IOException {
    BufferedReader fin = new BufferedReader(new FileReader(reportFile));
    String nodeName = fin.readLine();
    String readType = fin.readLine();
    ReadTaskReport ret = new ReadTaskReport(nodeName, readType);
    ret.mCores = Integer.parseInt(fin.readLine());
    ret.mTachyonWorkerBytes = Long.parseLong(fin.readLine());
    ret.mStartTimeMs = Long.parseLong(fin.readLine());
    ret.mFinishTimeMs = Long.parseLong(fin.readLine());
    ret.mSuccess = Boolean.parseBoolean(fin.readLine());
    int threadNum = Integer.parseInt(fin.readLine());
    if (threadNum >= 0) {
      ret.mThreadNum = threadNum;
      ret.mReadBytes = new long[threadNum];
      ret.mReadFiles = new int[threadNum];
      ret.mThreadTimeMs = new long[threadNum];
      for (int i = 0; i < threadNum; i ++) {
        ret.mThreadTimeMs[i] = Long.parseLong(fin.readLine());
        ret.mReadFiles[i] = Integer.parseInt(fin.readLine());
        ret.mReadBytes[i] = Long.parseLong(fin.readLine());
      }
    }
    fin.close();
    return ret;
  }

  private int mCores;
  private long mTachyonWorkerBytes;

  private long[] mReadBytes;
  private int[] mReadFiles;
  private int mThreadNum;
  private long[] mThreadTimeMs;
  private ReadType mReadType;

  protected ReadTaskReport(String nodeName, String readType) throws IOException {
    super(nodeName);
    mCores = Runtime.getRuntime().availableProcessors();
    mTachyonWorkerBytes = tachyon.conf.WorkerConf.get().MEMORY_SIZE;
    mThreadNum = -1;
    mReadType = ReadType.getOpType(readType);
  }

  public int getCores() {
    return mCores;
  }

  public ReadType getReadType() {
    return mReadType;
  }

  public long[] getReadBytes() {
    return mReadBytes;
  }

  public int[] getReadFiles() {
    return mReadFiles;
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

  public void setFromReadThreads(PerfThread[] readThreads) {
    mThreadNum = readThreads.length;
    mReadBytes = new long[mThreadNum];
    mReadFiles = new int[mThreadNum];
    mThreadTimeMs = new long[mThreadNum];
    for (int i = 0; i < mThreadNum; i ++) {
      RWThreadReport report = (RWThreadReport) readThreads[i].getReport();
      mReadBytes[i] = report.getSuccessBytes();
      mReadFiles[i] = report.getSuccessFiles();
      mThreadTimeMs[i] = report.getFinishTimeMs() - report.getStartTimeMs();
      if (!report.getSuccess()) {
        mSuccess = false;
      }
    }
  }

  @Override
  public void writeToFile(String fileName) throws IOException {
    File reportFile = new File(fileName);
    BufferedWriter fout = new BufferedWriter(new FileWriter(reportFile));
    fout.write(NODE_NAME + "\n");
    fout.write(mReadType.toString() + "\n");

    fout.write(mCores + "\n");
    fout.write(mTachyonWorkerBytes + "\n");

    fout.write(mStartTimeMs + "\n");
    fout.write(mFinishTimeMs + "\n");
    fout.write(mSuccess + "\n");
    fout.write(mThreadNum + "\n");
    if (mThreadNum >= 0) {
      for (int i = 0; i < mThreadNum; i ++) {
        fout.write(mThreadTimeMs[i] + "\n");
        fout.write(mReadFiles[i] + "\n");
        fout.write(mReadBytes[i] + "\n");
      }
    }
    fout.close();
  }
}
