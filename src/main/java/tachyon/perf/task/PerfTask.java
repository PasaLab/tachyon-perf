package tachyon.perf.task;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.client.TachyonFS;
import tachyon.org.apache.thrift.TException;
import tachyon.perf.PerfConstants;
import tachyon.perf.conf.PerfConf;

public abstract class PerfTask {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  public static PerfTask getPerfTask(String nodeName, int id, TaskType taskType, List<String> args)
      throws IOException {
    if (taskType.isRead()) {
      return new ReadTask(nodeName, id, args);
    } else if (taskType.isWrite()) {
      return new WriteTask(nodeName, id, args);
    } else {
      throw new IOException("Unsupport TaskType: " + taskType.toString());
    }
  }

  public final int ID;
  public final String NODE_NAME;
  public final TaskType TASK_TYPE;

  protected TaskReport mTaskReport;
  protected String mTfsFailedPath;
  protected String mTfsReadyPath;
  protected String mTfsSuccessPath;

  protected PerfTask(String nodeName, int id, TaskType taskType) {
    NODE_NAME = nodeName;
    ID = id;
    TASK_TYPE = taskType;
  }

  public boolean setup() {
    PerfConf perfConf = PerfConf.get();
    try {
      TachyonFS tfs = TachyonFS.get(perfConf.TFS_ADDRESS);
      if (tfs.exist(mTfsFailedPath)) {
        tfs.delete(mTfsFailedPath, true);
      }
      if (tfs.exist(mTfsReadyPath)) {
        tfs.delete(mTfsReadyPath, true);
      }
      if (tfs.exist(mTfsSuccessPath)) {
        tfs.delete(mTfsSuccessPath, true);
      }

      tfs.createFile(mTfsReadyPath);
      tfs.close();
    } catch (IOException e) {
      LOG.error("Error when setup task", e);
      return false;
    } catch (TException e) {
      LOG.warn("Failed to close the TachyonFS when setup task", e);
    }
    return true;
  }

  public abstract boolean start();

  public boolean generateReport() {
    mTaskReport.setSuccess(true);
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
      LOG.error("Error when generate the task report", e);
      return false;
    }
    return true;
  }

  public String getTfsFailedPath() {
    return mTfsFailedPath;
  }

  public String getTfsReadyPath() {
    return mTfsReadyPath;
  }

  public String getTfsSuccessPath() {
    return mTfsSuccessPath;
  }

  public boolean end() {
    PerfConf perfConf = PerfConf.get();
    try {
      TachyonFS tfs = TachyonFS.get(perfConf.TFS_ADDRESS);
      if (mTaskReport.getSuccess()) {
        tfs.createFile(mTfsSuccessPath);
      } else {
        tfs.createFile(mTfsFailedPath);
      }
      tfs.close();
    } catch (IOException e) {
      LOG.error("Error when end the task", e);
      return false;
    } catch (TException e) {
      LOG.warn("Failed to close the TachyonFs when end the task", e);
    }
    return true;
  }
}
