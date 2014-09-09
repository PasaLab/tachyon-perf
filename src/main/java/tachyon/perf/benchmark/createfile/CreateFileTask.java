package tachyon.perf.benchmark.createfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import tachyon.client.TachyonFS;
import tachyon.perf.basic.PerfTask;
import tachyon.perf.basic.Supervisible;
import tachyon.perf.basic.TaskContext;
import tachyon.perf.conf.PerfConf;

public class CreateFileTask extends PerfTask implements Supervisible {
  private CreateFileThread[] mCreateFileThreads;
  private List<Thread> mCreateFileThreadsList;

  @Override
  protected boolean setupTask(TaskContext taskContext) {
    PerfConf perfConf = PerfConf.get();
    ((CreateFileTaskContext) taskContext).initial();
    try {
      TachyonFS tfs = TachyonFS.get(perfConf.TFS_ADDRESS);
      String workDir = perfConf.TFS_DIR + "/CreateFile";
      if (!tfs.exist(workDir)) {
        tfs.mkdir(workDir);
        LOG.info("Create the work dir " + workDir);
      }
      int threadsNum = mTaskConf.getIntProperty("threads.num");
      mCreateFileThreads = new CreateFileThread[threadsNum];
      for (int i = 0; i < threadsNum; i++) {
        mCreateFileThreads[i] =
            new CreateFileThread(mId * threadsNum + i,
                mTaskConf.getIntProperty("files.per.thread"),
                mTaskConf.getIntProperty("file.length.bytes"), workDir);
      }
      LOG.info("Create " + threadsNum + " create file threads");
      tfs.close();
    } catch (IOException e) {
      LOG.error("Error when setup create file task", e);
      return false;
    }
    return true;
  }

  @Override
  protected boolean runTask(TaskContext taskContext) {
    mCreateFileThreadsList = new ArrayList<Thread>(mCreateFileThreads.length);
    for (int i = 0; i < mCreateFileThreads.length; i++) {
      Thread createFileThread = new Thread(mCreateFileThreads[i]);
      mCreateFileThreadsList.add(createFileThread);
      createFileThread.start();
    }
    try {
      boolean running = true;
      int interval = mTaskConf.getIntProperty("interval.seconds");
      int count = 0;
      ((CreateFileTaskContext) taskContext).UpdateSuccessFiles(0);
      while (running) {
        Thread.sleep(1000);
        running = false;
        count++;
        for (Thread thread : mCreateFileThreadsList) {
          if (thread.isAlive()) {
            running = true;
          }
        }
        if (!running || count % interval == 0) {
          int files = 0;
          for (CreateFileThread createFileThread : mCreateFileThreads) {
            files += createFileThread.getSuccessFiles();
          }
          ((CreateFileTaskContext) taskContext).UpdateSuccessFiles(files);
          count = 0;
        }
      }
    } catch (InterruptedException e) {
      LOG.error("Error when wait all threads", e);
      return false;
    }
    return true;
  }

  @Override
  protected boolean cleanupTask(TaskContext taskContext) {
    PerfConf perfConf = PerfConf.get();
    try {
      TachyonFS tfs = TachyonFS.get(perfConf.TFS_ADDRESS);
      String workDir = perfConf.TFS_DIR + "/CreateFile";
      tfs.delete(workDir, true);
      tfs.close();
    } catch (IOException e) {
      LOG.warn("Failed to clean up the work dir", e);
    }
    taskContext.setSuccess(true);
    return true;
  }

  @Override
  public String getTfsFailedPath() {
    return PerfConf.get().TFS_DIR + "/" + mId + "-FAILED";
  }

  @Override
  public String getTfsReadyPath() {
    return PerfConf.get().TFS_DIR + "/" + mId + "-READY";
  }

  @Override
  public String getTfsSuccessPath() {
    return PerfConf.get().TFS_DIR + "/" + mId + "-SUCCESS";
  }
}
