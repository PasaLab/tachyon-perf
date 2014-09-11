package tachyon.perf.basic;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import tachyon.client.TachyonFS;
import tachyon.perf.PerfConstants;
import tachyon.perf.conf.PerfConf;

/**
 * The abstract class for all the test tasks. For new test, you should create a new class which
 * extends this.
 */
public abstract class PerfTask {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  protected int mId;
  protected String mNodeName;
  protected TaskConfiguration mTaskConf;
  protected String mTaskType;

  public void initialSet(int id, String nodeName, String taskType, TaskConfiguration taskConf) {
    mId = id;
    mNodeName = nodeName;
    mTaskType = taskType;
    mTaskConf = taskConf;
  }

  /**
   * Setup the task. Do some preparations.
   * 
   * @param taskContext The statistics of this task
   * @return true if setup successfully, false otherwise
   */
  protected abstract boolean setupTask(TaskContext taskContext);

  /**
   * Run the task.
   * 
   * @param taskContext The statistics of this task
   * @return true if setup successfully, false otherwise
   */
  protected abstract boolean runTask(TaskContext taskContext);

  /**
   * Cleanup the task. Do some following work.
   * 
   * @param taskContext The statistics of this task
   * @return true if setup successfully, false otherwise
   */
  protected abstract boolean cleanupTask(TaskContext taskContext);

  public boolean setup(TaskContext taskContext) {
    taskContext.setStartTimeMs(System.currentTimeMillis());
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
      }
    }
    return setupTask(taskContext);
  }

  public boolean run(TaskContext taskContext) {
    if (this instanceof Supervisible) {
      try {
        TachyonFS tfs = TachyonFS.get(PerfConf.get().TFS_ADDRESS);
        String tfsReadyFilePath = ((Supervisible) this).getTfsReadyPath();
        tfs.createFile(tfsReadyFilePath);
        tfs.close();
      } catch (IOException e) {
        LOG.error("Failed to start Supervisible task", e);
        return false;
      }
    }
    return runTask(taskContext);
  }

  public boolean cleanup(TaskContext taskContext) {
    boolean ret = cleanupTask(taskContext);
    taskContext.setFinishTimeMs(System.currentTimeMillis());
    try {
      String outDirPath = PerfConf.get().OUT_FOLDER;
      File outDir = new File(outDirPath);
      if (!outDir.exists()) {
        outDir.mkdirs();
      }
      String reportFileName =
          outDirPath + "/" + PerfConstants.PERF_CONTEXT_FILE_NAME_PREFIX + mTaskType + "-" + mId
              + "@" + mNodeName;
      taskContext.writeToFile(reportFileName);
    } catch (IOException e) {
      LOG.error("Error when generate the task report", e);
      ret = false;
    }
    if (this instanceof Supervisible) {
      try {
        TachyonFS tfs = TachyonFS.get(PerfConf.get().TFS_ADDRESS);
        String tfsFailedFilePath = ((Supervisible) this).getTfsFailedPath();
        String tfsSuccessFilePath = ((Supervisible) this).getTfsSuccessPath();
        if (taskContext.getSuccess() && ret) {
          tfs.createFile(tfsSuccessFilePath);
        } else {
          tfs.createFile(tfsFailedFilePath);
        }
        tfs.close();
      } catch (IOException e) {
        LOG.error("Failed to start Supervisible task", e);
        ret = false;
      }
    }
    return ret;
  }
}
