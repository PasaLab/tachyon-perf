package tachyon.perf.task;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.org.apache.thrift.TException;
import tachyon.perf.PerfConstants;
import tachyon.perf.conf.PerfConf;
import tachyon.perf.thread.PerfThread;
import tachyon.perf.thread.WriteThread;

public class WriteTask extends PerfTask {
  private final WriteType WRITE_TYPE;

  private PerfThread[] mWriteThreads;
  private List<Thread> mWriteThreadsList;

  protected WriteTask(String nodeName, int id, List<String> args) throws IOException {
    super(nodeName, id, TaskType.Write);
    if (args.size() < 1) {
      throw new IOException("Error when new WriteTask: not enough args.");
    }
    WRITE_TYPE = WriteType.getOpType(args.get(0));
    mTaskReport = new RWTaskReport(nodeName, args.get(0));
    PerfConf perfConf = PerfConf.get();
    mTfsFailedPath = perfConf.TFS_DIR + "/" + ID + "/FAILED";
    mTfsReadyPath = perfConf.TFS_DIR + "/" + ID + "/READY";
    mTfsSuccessPath = perfConf.TFS_DIR + "/" + ID + "/SUCCESS";
  }

  @Override
  public boolean generateReport() {
    mTaskReport.setSuccess(true);
    ((RWTaskReport) mTaskReport).setFromRWThreads(mWriteThreads);
    mTaskReport.setFinishTimeMs(System.currentTimeMillis());
    String outDirPath = PerfConf.get().OUT_FOLDER;
    File outDir = new File(outDirPath);
    if (!outDir.exists()) {
      outDir.mkdirs();
    }
    String reportFileName =
        outDirPath + "/" + PerfConstants.PERF_REPORT_FILE_NAME_PREFIX + "-" + TASK_TYPE.toString();
    try {
      mTaskReport.writeToFile(reportFileName);
    } catch (IOException e) {
      LOG.error("Error when generate the write task report", e);
      return false;
    }
    return true;
  }

  @Override
  public boolean setup() {
    PerfConf perfConf = PerfConf.get();
    try {
      TachyonFS tfs = TachyonFS.get(perfConf.TFS_ADDRESS);
      String writeDir = perfConf.TFS_DIR + "/" + ID;
      if (tfs.exist(writeDir)) {
        tfs.delete(writeDir, true);
        LOG.warn("The write dir " + writeDir + " already exists, delete it");
      }
      tfs.mkdir(writeDir);
      LOG.info("Create the write dir " + writeDir);

      int threadsNum = perfConf.WRITE_THREADS_NUM;
      List<String>[] writeFileList =
          ListGenerator.generateWriteFiles(threadsNum, perfConf.WRITE_FILES_PER_THREAD, writeDir);
      mWriteThreads = new PerfThread[threadsNum];
      for (int i = 0; i < threadsNum; i ++) {
        mWriteThreads[i] = new WriteThread(i, writeFileList[i], WRITE_TYPE);
      }
      LOG.info("Create " + threadsNum + " write threads");

      tfs.createFile(mTfsReadyPath);
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
  public boolean start() {
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
}
