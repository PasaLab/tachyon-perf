package tachyon.perf.benchmark.read;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.org.apache.thrift.TException;
import tachyon.perf.basic.PerfTask;
import tachyon.perf.basic.Supervisible;
import tachyon.perf.basic.TaskContext;
import tachyon.perf.benchmark.ListGenerator;
import tachyon.perf.conf.PerfConf;

/**
 * The read test task. It will read files from Tachyon in multi-thread.
 */
public class ReadTask extends PerfTask implements Supervisible {
  private ReadThread[] mReadThreads;
  private List<Thread> mReadThreadsList;
  private ReadType mReadType;

  @Override
  protected boolean cleanupTask(TaskContext taskContext) {
    taskContext.setSuccess(true);
    ((ReadTaskContext) taskContext).setFromReadThreads(mReadThreads);
    return true;
  }

  @Override
  protected boolean setupTask(TaskContext taskContext) {
    PerfConf perfConf = PerfConf.get();
    try {
      mReadType = ReadType.getOpType(mTaskConf.getProperty("read.type"));
      ((ReadTaskContext) taskContext).initial(mReadType);
      TachyonFS tfs = TachyonFS.get(perfConf.TFS_ADDRESS);
      String readDir = perfConf.TFS_DIR + "/" + mId;
      if (!tfs.exist(readDir)) {
        LOG.error("The read dir " + readDir + " is not exist. " + "Do the write test fisrt");
        return false;
      }
      if (tfs.getFile(readDir).isFile()) {
        LOG.error("The read dir " + readDir + " is not a directory. " + "Do the write test fisrt");
        return false;
      }
      List<Integer> readFileCandidates = tfs.listFiles(readDir, true);
      if (readFileCandidates.isEmpty()) {
        LOG.error("The read dir " + readDir + " is empty");
        return false;
      }
      LOG.info("The read dir " + readDir + ", contains " + readFileCandidates.size() + " files");

      ReadMode readMode = ReadMode.getReadMode(mTaskConf.getProperty("mode"));
      int threadsNum = mTaskConf.getIntProperty("threads.num");
      int grainBytes = mTaskConf.getIntProperty("grain.bytes");
      List<Integer>[] readFileList =
          ListGenerator.generateReadFiles(threadsNum, mTaskConf.getIntProperty("files.per.thread"),
              readFileCandidates, readMode, mTaskConf.getBooleanProperty("indentical"));
      mReadThreads = new ReadThread[threadsNum];
      for (int i = 0; i < threadsNum; i++) {
        mReadThreads[i] = new ReadThread(i, readFileList[i], mReadType, grainBytes);
      }
      LOG.info("Create " + threadsNum + " read threads");
      tfs.close();
    } catch (IOException e) {
      LOG.error("Error when setup read task", e);
      return false;
    } catch (TException e) {
      LOG.warn("Failed to close the TachyonFS when setup read task", e);
    }
    return true;
  }

  @Override
  protected boolean runTask(TaskContext taskContext) {
    mReadThreadsList = new ArrayList<Thread>(mReadThreads.length);
    for (int i = 0; i < mReadThreads.length; i++) {
      Thread readThread = new Thread(mReadThreads[i]);
      mReadThreadsList.add(readThread);
      readThread.start();
    }
    try {
      for (Thread thread : mReadThreadsList) {
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
