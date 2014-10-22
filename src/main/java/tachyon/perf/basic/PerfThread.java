package tachyon.perf.basic;

import org.apache.log4j.Logger;

import tachyon.perf.PerfConstants;

public abstract class PerfThread implements Runnable {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  protected int mId;
  protected int mTaskId;
  protected String mNodeName;
  protected String mTaskType;

  public void initialSet(int threadId, int taskId, String nodeName, String taskType) {
    mId = threadId;
    mTaskId = taskId;
    mNodeName = nodeName;
    mTaskType = taskType;
  }

  /**
   * Setup the thread. Do some preparations.
   * 
   * @param taskConf
   * @return true if setup successfully, false otherwise
   */
  public abstract boolean setupThread(TaskConfiguration taskConf);

  /**
   * Cleanup the thread. Do some following work.
   * 
   * @param taskConf
   * @return true if cleanup successfully, false otherwise
   */
  public abstract boolean cleanupThread(TaskConfiguration taskConf);
}
