package tachyon.perf.basic;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.client.TachyonFS;
import tachyon.org.apache.thrift.TException;
import tachyon.perf.PerfConstants;
import tachyon.perf.benchmark.read.ReadTask;
import tachyon.perf.benchmark.write.WriteTask;
import tachyon.perf.conf.PerfConf;

/**
 * The abstract class for all the test tasks. For new test, you should create a new class which
 * extends this.
 */
public abstract class PerfTask {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  public static PerfTask
      getPerfTask(String nodeName, int id, TaskType taskType, List<String> args)
          throws IOException {
    PerfTask ret = null;
    if (taskType.isRead()) {
      ret = new ReadTask(nodeName, id, args);
    } else if (taskType.isWrite()) {
      ret = new WriteTask(nodeName, id, args);
    }
    /* Add your own Task here */
    else {
      throw new IOException("Unsupport TaskType: " + taskType.toString());
    }
    ret.mId = id;
    ret.mNodeName = nodeName;
    ret.mTaskType = taskType;
    return ret;
  }

  private int mId;
  private String mNodeName;
  private TaskType mTaskType;

  protected PerfTask() {
  }

  public int getId() {
    return mId;
  }

  public String getNodeName() {
    return mNodeName;
  }

  public TaskType getTaskType() {
    return mTaskType;
  }

  /**
   * Setup the task. Do some preparations.
   * 
   * @param taskReport
   *          The statistics of this task
   * @return true if setup successfully, false otherwise
   */
  protected abstract boolean setupTask(TaskReport taskReport);

  /**
   * Run the task.
   * 
   * @param taskReport
   *          The statistics of this task
   * @return true if setup successfully, false otherwise
   */
  protected abstract boolean runTask(TaskReport taskReport);

  /**
   * Cleanup the task. Do some following work.
   * 
   * @param taskReport
   *          The statistics of this task
   * @return true if setup successfully, false otherwise
   */
  protected abstract boolean cleanupTask(TaskReport taskReport);

  public boolean setup(TaskReport taskReport) {
    taskReport.setStartTimeMs(System.currentTimeMillis());
    if (this instanceof Supervisible) {
      try {
        TachyonFS tfs = TachyonFS.get(PerfConf.get().TFS_ADDRESS);
        String tfsFailedFilePath = ((Supervisible) this).getTfsFailedPath();
        String tfsReadyFilePath = ((Supervisible) this).getTfsReadyPath();
        String tfsSuccessFilePath = ((Supervisible) this).getTfsSuccessPath();
        if (tfs.exist(tfsFailedFilePath)) {
          tfs.delete(tfsFailedFilePath, true);
        }
        if (tfs.exist(tfsReadyFilePath)) {
          tfs.delete(tfsReadyFilePath, true);
        }
        if (tfs.exist(tfsSuccessFilePath)) {
          tfs.delete(tfsSuccessFilePath, true);
        }
        tfs.close();
      } catch (IOException e) {
        LOG.error("Failed to setup Supervisible task", e);
        return false;
      } catch (TException e) {
        LOG.warn("Error when close TachyonFS", e);
      }
    }
    return setupTask(taskReport);
  }

  public boolean run(TaskReport taskReport) {
    if (this instanceof Supervisible) {
      try {
        TachyonFS tfs = TachyonFS.get(PerfConf.get().TFS_ADDRESS);
        String tfsReadyFilePath = ((Supervisible) this).getTfsReadyPath();
        tfs.createFile(tfsReadyFilePath);
        tfs.close();
      } catch (IOException e) {
        LOG.error("Failed to start Supervisible task", e);
        return false;
      } catch (TException e) {
        LOG.warn("Error when close TachyonFS", e);
      }
    }
    return runTask(taskReport);
  }

  public boolean cleanup(TaskReport taskReport) {
    boolean ret = cleanupTask(taskReport);
    taskReport.setFinishTimeMs(System.currentTimeMillis());
    try {
      String outDirPath = PerfConf.get().OUT_FOLDER;
      File outDir = new File(outDirPath);
      if (!outDir.exists()) {
        outDir.mkdirs();
      }
      String reportFileName =
          outDirPath + "/" + PerfConstants.PERF_REPORT_FILE_NAME_PREFIX + "-"
              + mTaskType.toString();
      taskReport.writeToFile(reportFileName);
    } catch (IOException e) {
      LOG.error("Error when generate the task report", e);
      ret = false;
    }
    if (this instanceof Supervisible) {
      try {
        TachyonFS tfs = TachyonFS.get(PerfConf.get().TFS_ADDRESS);
        String tfsFailedFilePath = ((Supervisible) this).getTfsFailedPath();
        String tfsSuccessFilePath = ((Supervisible) this).getTfsSuccessPath();
        if (taskReport.getSuccess() && ret) {
          tfs.createFile(tfsSuccessFilePath);
        } else {
          tfs.createFile(tfsFailedFilePath);
        }
        tfs.close();
      } catch (IOException e) {
        LOG.error("Failed to start Supervisible task", e);
        ret = false;
      } catch (TException e) {
        LOG.warn("Error when close TachyonFS", e);
      }
    }
    return ret;
  }
}
