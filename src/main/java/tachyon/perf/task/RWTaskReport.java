package tachyon.perf.task;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import tachyon.perf.thread.PerfThread;
import tachyon.perf.thread.RWThreadReport;

public class RWTaskReport extends TaskReport {
  public static RWTaskReport loadFromFile(File reportFile) throws IOException {
    BufferedReader fin = new BufferedReader(new FileReader(reportFile));
    String nodeName = fin.readLine();
    String rwType = fin.readLine();
    RWTaskReport ret = new RWTaskReport(nodeName, rwType);
    ret.mCores = Integer.parseInt(fin.readLine());
    ret.mTachyonWorkerBytes = Long.parseLong(fin.readLine());
    ret.mStartTimeMs = Long.parseLong(fin.readLine());
    ret.mFinishTimeMs = Long.parseLong(fin.readLine());
    ret.mSuccess = Boolean.parseBoolean(fin.readLine());
    int threadNum = Integer.parseInt(fin.readLine());
    if (threadNum >= 0) {
      ret.mThreadNum = threadNum;
      ret.mSuccessBytes = new long[threadNum];
      ret.mSuccessFiles = new int[threadNum];
      ret.mThreadTimeMs = new long[threadNum];
      for (int i = 0; i < threadNum; i ++) {
        ret.mThreadTimeMs[i] = Long.parseLong(fin.readLine());
        ret.mSuccessFiles[i] = Integer.parseInt(fin.readLine());
        ret.mSuccessBytes[i] = Long.parseLong(fin.readLine());
      }
    }
    fin.close();
    return ret;
  }

  private int mCores;
  private long mTachyonWorkerBytes;

  private long[] mSuccessBytes;
  private int[] mSuccessFiles;
  private int mThreadNum;
  private long[] mThreadTimeMs;
  private String mRWType;

  public RWTaskReport(String nodeName, String rwType) {
    super(nodeName);
    mCores = Runtime.getRuntime().availableProcessors();
    mTachyonWorkerBytes = tachyon.conf.WorkerConf.get().MEMORY_SIZE;
    mThreadNum = -1;
    mRWType = rwType;
  }

  public int getCores() {
    return mCores;
  }

  public String getRWType() {
    return mRWType;
  }

  public long[] getSuccessBytes() {
    return mSuccessBytes;
  }

  public int[] getSuccessFiles() {
    return mSuccessFiles;
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

  public void setFromRWThreads(PerfThread[] rwThreads) {
    mThreadNum = rwThreads.length;
    mSuccessBytes = new long[mThreadNum];
    mSuccessFiles = new int[mThreadNum];
    mThreadTimeMs = new long[mThreadNum];
    for (int i = 0; i < mThreadNum; i ++) {
      RWThreadReport report = (RWThreadReport) rwThreads[i].getReport();
      mSuccessBytes[i] = report.getSuccessBytes();
      mSuccessFiles[i] = report.getSuccessFiles();
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
    fout.write(mRWType + "\n");

    fout.write(mCores + "\n");
    fout.write(mTachyonWorkerBytes + "\n");

    fout.write(mStartTimeMs + "\n");
    fout.write(mFinishTimeMs + "\n");
    fout.write(mSuccess + "\n");
    fout.write(mThreadNum + "\n");
    if (mThreadNum >= 0) {
      for (int i = 0; i < mThreadNum; i ++) {
        fout.write(mThreadTimeMs[i] + "\n");
        fout.write(mSuccessFiles[i] + "\n");
        fout.write(mSuccessBytes[i] + "\n");
      }
    }
    fout.close();
  }
}