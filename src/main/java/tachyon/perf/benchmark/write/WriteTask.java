package tachyon.perf.benchmark.write;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.org.apache.thrift.TException;
import tachyon.perf.basic.PerfTask;
import tachyon.perf.basic.Supervisible;
import tachyon.perf.basic.TaskReport;
import tachyon.perf.benchmark.ListGenerator;
import tachyon.perf.conf.PerfConf;
import tachyon.perf.conf.PerfTaskConf;

/**
 * The write test task. It will write files to Tachyon in multi-thread.
 */
public class WriteTask extends PerfTask implements Supervisible {
  private final WriteType WRITE_TYPE;

  private WriteThread[] mWriteThreads;
  private List<Thread> mWriteThreadsList;

  public WriteTask(String nodeName, int id, List<String> args) throws IOException {
    super();
    if (args.size() < 1) {
      throw new IOException("Error when new WriteTask: not enough args.");
    }
    WRITE_TYPE = WriteType.getOpType(args.get(0));
  }

  @Override
  protected boolean cleanupTask(TaskReport taskReport) {
    taskReport.setSuccess(true);
    ((WriteTaskReport) taskReport).setFromWriteThreads(mWriteThreads);
    return true;
  }

  @Override
  protected boolean setupTask(TaskReport taskReport) {
    PerfConf perfConf = PerfConf.get();
    PerfTaskConf perfTaskConf = PerfTaskConf.get();
    try {
      TachyonFS tfs = TachyonFS.get(perfConf.TFS_ADDRESS);
      String writeDir = perfConf.TFS_DIR + "/" + getId();
      if (tfs.exist(writeDir)) {
        tfs.delete(writeDir, true);
        LOG.warn("The write dir " + writeDir + " already exists, delete it");
      }
      tfs.mkdir(writeDir);
      LOG.info("Create the write dir " + writeDir);

      int threadsNum = perfTaskConf.WRITE_THREADS_NUM;
      List<String>[] writeFileList =
          ListGenerator.generateWriteFiles(threadsNum, perfTaskConf.WRITE_FILES_PER_THREAD,
              writeDir);
      mWriteThreads = new WriteThread[threadsNum];
      for (int i = 0; i < threadsNum; i ++) {
        mWriteThreads[i] = new WriteThread(i, writeFileList[i], WRITE_TYPE);
      }
      LOG.info("Create " + threadsNum + " write threads");
      tfs.close();
    } catch (IOException e) {
      LOG.error("Error when setup write task", e);
      return false;
    } catch (TException e) {
      LOG.warn("Failed to close the TachyonFS when setup write task", e);
    }
    return true;
  }

  @Override
  protected boolean runTask(TaskReport taskReport) {
    mWriteThreadsList = new ArrayList<Thread>(mWriteThreads.length);
    for (int i = 0; i < mWriteThreads.length; i ++) {
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
  public String getTfsFailedPath() {
    return PerfConf.get().TFS_DIR + "/" + getId() + "/FAILED";
  }

  @Override
  public String getTfsReadyPath() {
    return PerfConf.get().TFS_DIR + "/" + getId() + "/READY";
  }

  @Override
  public String getTfsSuccessPath() {
    return PerfConf.get().TFS_DIR + "/" + getId() + "/SUCCESS";
  }
}
