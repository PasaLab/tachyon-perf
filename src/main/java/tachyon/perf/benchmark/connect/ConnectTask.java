package tachyon.perf.benchmark.connect;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import tachyon.client.TachyonFS;
import tachyon.org.apache.thrift.TException;
import tachyon.perf.basic.PerfTask;
import tachyon.perf.basic.Supervisible;
import tachyon.perf.basic.TaskContext;
import tachyon.perf.conf.PerfConf;

public class ConnectTask extends PerfTask implements Supervisible {
  private ConnectThread[] mConnectThreads;
  private List<Thread> mConnectThreadsList;

  @Override
  protected boolean cleanupTask(TaskContext taskContext) {
    taskContext.setSuccess(true);
    ((ConnectTaskContext) taskContext).setFromConnectThreads(mConnectThreads);
    return true;
  }

  @Override
  protected boolean setupTask(TaskContext taskContext) {
    PerfConf perfConf = PerfConf.get();
    try {
      TachyonFS tfs = TachyonFS.get(perfConf.TFS_ADDRESS);
      String workDir = perfConf.TFS_DIR + "/" + mId;
      if (tfs.exist(workDir)) {
        tfs.delete(workDir, true);
        LOG.warn("The work dir " + workDir + " already exists, delete it");
      }
      tfs.mkdir(workDir);
      LOG.info("Create the write dir " + workDir);

      int threadsNum = mTaskConf.getIntProperty("threads.num");
      int clientsPerThread = mTaskConf.getIntProperty("clients.per.thread");
      int opsPerThread = mTaskConf.getIntProperty("ops.per.thread");
      mConnectThreads = new ConnectThread[threadsNum];
      for (int i = 0; i < threadsNum; i++) {
        mConnectThreads[i] = new ConnectThread(i, clientsPerThread, opsPerThread, workDir);
      }
      LOG.info("Create " + threadsNum + " connect threads");
      tfs.close();
    } catch (TException e) {
      LOG.warn("Failed to close TachyonFS", e);
    } catch (IOException e) {
      LOG.error("Error when setup connect task", e);
      return false;
    }
    return true;
  }

  @Override
  protected boolean runTask(TaskContext taskContext) {
    mConnectThreadsList = new ArrayList<Thread>(mConnectThreads.length);
    for (int i = 0; i < mConnectThreads.length; i++) {
      Thread connectThread = new Thread(mConnectThreads[i]);
      mConnectThreadsList.add(connectThread);
      connectThread.start();
    }
    try {
      for (Thread thread : mConnectThreadsList) {
        thread.join();
      }
    } catch (InterruptedException e) {
      LOG.error("Error when wait all threads", e);
      return false;
    }
    return true;
  }

  @Override
  public String cleanupWorkspace() {
    return PerfConf.get().TFS_DIR;
  }
  
  @Override
  public String getTfsFailedPath() {
    return PerfConf.get().TFS_DIR + "/" + mId + "/FAILED";
  }

  @Override
  public String getTfsReadyPath() {
    return PerfConf.get().TFS_DIR + "/" + mId + "/READY";
  }

  @Override
  public String getTfsSuccessPath() {
    return PerfConf.get().TFS_DIR + "/" + mId + "/SUCCESS";
  }
}
