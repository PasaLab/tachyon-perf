package tachyon.perf.basic;

import java.io.IOException;

import org.apache.log4j.Logger;

import tachyon.perf.PerfConstants;

/**
 * The abstract class for all test statistics. For new test, you should implement your own
 * TaskReport.
 */
public abstract class TaskContext {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  protected int mId;
  protected String mNodeName;
  protected String mTaskType;

  protected long mFinishTimeMs;
  protected long mStartTimeMs;
  protected boolean mSuccess;

  public void initialSet(int id, String nodeName, String taskType) {
    mId = id;
    mNodeName = nodeName;
    mTaskType = taskType;
    mStartTimeMs = System.currentTimeMillis();
    mFinishTimeMs = mStartTimeMs;
    mSuccess = false;
  }

  public int getId() {
    return mId;
  }

  public String getNodeName() {
    return mNodeName;
  }

  public String getTaskType() {
    return mTaskType;
  }

  public long getFinishTimeMs() {
    return mFinishTimeMs;
  }

  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  public boolean getSuccess() {
    return mSuccess;
  }

  public void setStartTimeMs(long startTimeMs) {
    mStartTimeMs = startTimeMs;
  }

  public void setFinishTimeMs(long finishTimeMs) {
    mFinishTimeMs = finishTimeMs;
  }

  public void setSuccess(boolean success) {
    mSuccess = success;
  }

  /**
   * Output this task report to file.
   * 
   * @param fileName
   *          the name of the output file
   * @throws IOException
   */
  public abstract void writeToFile(String fileName) throws IOException;
}
