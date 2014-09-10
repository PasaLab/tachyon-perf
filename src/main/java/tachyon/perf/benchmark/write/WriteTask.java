package tachyon.perf.benchmark.write;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.org.apache.thrift.TException;
import tachyon.perf.basic.PerfTask;
import tachyon.perf.basic.Supervisible;
import tachyon.perf.basic.TaskContext;
import tachyon.perf.benchmark.ListGenerator;
import tachyon.perf.conf.PerfConf;

/**
 * The write test task. It will write files to Tachyon in multi-thread.
 */
public class WriteTask extends PerfTask implements Supervisible {
  private WriteThread[] mWriteThreads;
  private List<Thread> mWriteThreadsList;
  private WriteType mWriteType;

  @Override
  protected boolean cleanupTask(TaskContext taskContext) {
    taskContext.setSuccess(true);
    ((WriteTaskContext) taskContext).setFromWriteThreads(mWriteThreads);
    return true;
  }

  @Override
  protected boolean setupTask(TaskContext taskContext) {
    PerfConf perfConf = PerfConf.get();
    try {
      mWriteType = WriteType.getOpType(mTaskConf.getProperty("write.type"));
      ((WriteTaskContext) taskContext).initial(mWriteType);
      TachyonFS tfs = TachyonFS.get(perfConf.TFS_ADDRESS);
      String writeDir = perfConf.TFS_DIR + "/" + mId;
      if (tfs.exist(writeDir)) {
        tfs.delete(writeDir, true);
        LOG.warn("The write dir " + writeDir + " already exists, delete it");
      }
      tfs.mkdir(writeDir);
      LOG.info("Create the write dir " + writeDir);

      int threadsNum = mTaskConf.getIntProperty("threads.num");
      List<String>[] writeFileList =
          ListGenerator.generateWriteFiles(threadsNum,
              mTaskConf.getIntProperty("files.per.thread"), writeDir);
      mWriteThreads = new WriteThread[threadsNum];
      for (int i = 0; i < threadsNum; i++) {
        mWriteThreads[i] =
            new WriteThread(i, writeFileList[i], mWriteType,
                mTaskConf.getLongProperty("file.length.bytes"),
                mTaskConf.getIntProperty("grain.bytes"));
      }
      LOG.info("Create " + threadsNum + " write threads");
      tfs.close();
    } catch (IOException e) {
      LOG.error("Error when setup write task", e);
      return false;
    }
    return true;
  }

  @Override
  protected boolean runTask(TaskContext taskContext) {
    mWriteThreadsList = new ArrayList<Thread>(mWriteThreads.length);
    for (int i = 0; i < mWriteThreads.length; i++) {
      Thread writeThread = new Thread(mWriteThreads[i]);
      mWriteThreadsList.add(writeThread);
      writeThread.start();
    }
    try {
      for (Thread thread : mWriteThreadsList) {
        thread.join();
      }
    } catch (InterruptedException e) {
      LOG.error("Error when wait all threads", e);
      return false;
    }
    return true;
  }

  @Override
  public boolean cleanupWorkspace() {
    return false;
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
