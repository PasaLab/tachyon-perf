package tachyon.perf.benchmark.connect;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import tachyon.perf.basic.TaskContext;

public class ConnectTaskContext extends TaskContext {
  public static ConnectTaskContext loadFromFile(File contextFile) throws IOException {
    ConnectTaskContext ret = new ConnectTaskContext();
    BufferedReader fin = new BufferedReader(new FileReader(contextFile));
    ret.mNodeName = fin.readLine();
    ret.mCores = Integer.parseInt(fin.readLine());
    ret.mStartTimeMs = Long.parseLong(fin.readLine());
    ret.mFinishTimeMs = Long.parseLong(fin.readLine());
    ret.mSuccess = Boolean.parseBoolean(fin.readLine());
    int threadNum = Integer.parseInt(fin.readLine());
    if (threadNum >= 0) {
      ret.mThreadNum = threadNum;
      ret.mOps = new int[threadNum];
      ret.mThreadTimeMs = new long[threadNum];
      for (int i = 0; i < threadNum; i++) {
        ret.mThreadTimeMs[i] = Long.parseLong(fin.readLine());
        ret.mOps[i] = Integer.parseInt(fin.readLine());
      }
    }
    fin.close();
    return ret;
  }

  private int mCores;
  private int[] mOps;
  private int mThreadNum = -1;
  private long[] mThreadTimeMs;

  public int getCores() {
    return mCores;
  }

  public int[] getOps() {
    return mOps;
  }

  public int getThreadNum() {
    return mThreadNum;
  }

  public long[] getThreadTimeMs() {
    return mThreadTimeMs;
  }

  public void setFromConnectThreads(ConnectThread[] connectThreads) {
    mCores = Runtime.getRuntime().availableProcessors();
    mThreadNum = connectThreads.length;
    mOps = new int[mThreadNum];
    mThreadTimeMs = new long[mThreadNum];
    for (int i = 0; i < mThreadNum; i++) {
      ConnectThreadStatistic statistics = connectThreads[i].getStatistic();
      mOps[i] = statistics.getSuccessOps();
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
    fout.write(mNodeName + "\n");
    fout.write(mCores + "\n");

    fout.write(mStartTimeMs + "\n");
    fout.write(mFinishTimeMs + "\n");
    fout.write(mSuccess + "\n");
    fout.write(mThreadNum + "\n");
    if (mThreadNum >= 0) {
      for (int i = 0; i < mThreadNum; i++) {
        fout.write(mThreadTimeMs[i] + "\n");
        fout.write(mOps[i] + "\n");
      }
    }
    fout.close();
  }
}
