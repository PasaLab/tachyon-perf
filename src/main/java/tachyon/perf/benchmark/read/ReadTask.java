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
import tachyon.perf.conf.PerfTaskConf;

/**
 * The read test task. It will read files from Tachyon in multi-thread.
 */
public class ReadTask extends PerfTask implements Supervisible {
  private final ReadType READ_TYPE;

  private ReadThread[] mReadThreads;
  private List<Thread> mReadThreadsList;

  public ReadTask(String nodeName, int id, List<String> args) throws IOException {
    super();
    if (args.size() < 1) {
      throw new IOException("Error when new ReadTask: not enough args.");
    }
    READ_TYPE = ReadType.getOpType(args.get(0));
  }

  @Override
  protected boolean cleanupTask(TaskContext taskContext) {
    taskContext.setSuccess(true);
    ((ReadTaskContext) taskContext).setFromReadThreads(mReadThreads);
    return true;
  }

  @Override
  protected boolean setupTask(TaskContext taskContext) {
    PerfConf perfConf = PerfConf.get();
    PerfTaskConf perfTaskConf = PerfTaskConf.get();
    try {
      TachyonFS tfs = TachyonFS.get(perfConf.TFS_ADDRESS);
      String readDir = perfConf.TFS_DIR + "/" + getId();
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

      ReadMode readMode = ReadMode.getReadMode(perfTaskConf.READ_MODE);
      int threadsNum = perfTaskConf.READ_THREADS_NUM;
      List<Integer>[] readFileList =
          ListGenerator.generateReadFiles(threadsNum, perfTaskConf.READ_FILES_PER_THREAD,
              readFileCandidates, readMode, perfTaskConf.READ_IDENTICAL);
      mReadThreads = new ReadThread[threadsNum];
      for (int i = 0; i < threadsNum; i ++) {
        mReadThreads[i] = new ReadThread(i, readFileList[i], READ_TYPE);
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
    for (int i = 0; i < mReadThreads.length; i ++) {
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
