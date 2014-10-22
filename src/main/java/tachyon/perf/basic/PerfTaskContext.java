package tachyon.perf.basic;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import tachyon.perf.PerfConstants;

/**
 * The abstract class for all test statistics. For new test, you should implement your own
 * PerfTaskContext.
 */
public abstract class PerfTaskContext {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  protected int mId;
  protected String mNodeName;
  protected String mTaskType;

  protected long mFinishTimeMs;
  protected long mStartTimeMs;
  protected boolean mSuccess;

  public void initial(int id, String nodeName, String taskType, TaskConfiguration taskConf) {
    mId = id;
    mNodeName = nodeName;
    mTaskType = taskType;
    mStartTimeMs = System.currentTimeMillis();
    mFinishTimeMs = mStartTimeMs;
    mSuccess = true;
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
   * Load this task context from file
   * 
   * @param file the input file
   * @throws IOException
   */
  public abstract void loadFromFile(File file) throws IOException;

  /**
   * Set contexts from test threads.
   * 
   * @param threads
   */
  public abstract void setFromThread(PerfThread[] threads);

  /**
   * Output this task context to file.
   * 
   * @param file the output file
   * @throws IOException
   */
  public abstract void writeToFile(File file) throws IOException;
}
