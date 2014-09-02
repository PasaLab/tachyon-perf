package tachyon.perf.benchmark.connect;

import java.io.IOException;

import org.apache.log4j.Logger;

import tachyon.client.TachyonFS;
import tachyon.org.apache.thrift.TException;
import tachyon.perf.PerfConstants;
import tachyon.perf.conf.PerfConf;

public class ConnectThread implements Runnable {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  private static final int OP_DELETE = 1;
  private static final int OP_GET = 2;
  private static final int OP_MKDIR = 3;
  private static final int OP_RENAME = 4;

  public final int ID;

  private TachyonFS[] mClients;
  private int mOps;
  private ConnectThreadStatistic mStatistic;
  private String mTfsAddress;
  private String mTargetDir;

  public ConnectThread(int id, int clients, int ops, String workDir) {
    ID = id;
    mClients = new TachyonFS[clients];
    mOps = ops;
    mStatistic = new ConnectThreadStatistic();
    mTfsAddress = PerfConf.get().TFS_ADDRESS;
    mTargetDir = workDir + "/" + ID;
  }

  public ConnectThreadStatistic getStatistic() {
    return mStatistic;
  }

  @Override
  public void run() {
    mStatistic.setStartTimeMs(System.currentTimeMillis());
    try {
      for (int i = 0; i < mClients.length; i++) {
        mClients[i] = TachyonFS.get(mTfsAddress);
      }
    } catch (IOException e) {
      LOG.error("Connect Thread " + ID + " falied to connect Tachyon", e);
      mStatistic.setFinishTimeMs(System.currentTimeMillis());
      return;
    }
    int nextClient = 0;
    int nextOp = OP_MKDIR;
    int fileId = -1;
    try {
      for (int i = 0; i < mOps; i++) {
        if (nextOp == OP_DELETE) {
          mClients[nextClient].delete(fileId, true);
          nextOp = OP_MKDIR;
        } else if (nextOp == OP_GET) {
          fileId = mClients[nextClient].getFileId(mTargetDir);
          nextOp = OP_RENAME;
        } else if (nextOp == OP_MKDIR) {
          mClients[nextClient].mkdir(mTargetDir);
          nextOp = OP_GET;
        } else if (nextOp == OP_RENAME) {
          mClients[nextClient].rename(fileId, mTargetDir + "-");
          nextOp = OP_DELETE;
        }
        nextClient = (nextClient + 1) % mClients.length;
        mStatistic.addOps(1);
      }
    } catch (IOException e) {
      LOG.error("Connect thread " + ID + " failed to operate on Tachyon", e);
      mStatistic.setFinishTimeMs(System.currentTimeMillis());
      return;
    }
    for (int i = 0; i < mClients.length; i++) {
      try {
        mClients[i].close();
      } catch (TException e) {
        LOG.warn("Connect Thread " + ID + " falied to close TachyonFS", e);
      }
    }
    mStatistic.setSuccess(true);
    mStatistic.setFinishTimeMs(System.currentTimeMillis());
  }
}
