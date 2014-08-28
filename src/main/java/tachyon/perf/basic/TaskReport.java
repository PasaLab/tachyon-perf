package tachyon.perf.basic;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.perf.PerfConstants;
import tachyon.perf.benchmark.read.ReadTaskReport;
import tachyon.perf.benchmark.write.WriteTaskReport;

/**
 * The abstract class for all test statistics. For new test, you should implement your own
 * TaskReport.
 */
public abstract class TaskReport {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  public static TaskReport getTaskReport(String nodeName, int id, TaskType taskType,
      List<String> args) throws IOException {
    TaskReport ret = null;
    if (taskType.isRead()) {
      ret = new ReadTaskReport(nodeName, args.get(0));
    } else if (taskType.isWrite()) {
      ret = new WriteTaskReport(nodeName, args.get(0));
    }
    /* Add your own TaskReport here */
    else {
      throw new IOException("Unsupport TaskType: " + taskType.toString());
    }
    return ret;
  }

  public final String NODE_NAME;

  protected long mFinishTimeMs;
  protected long mStartTimeMs;
  protected boolean mSuccess;

  protected TaskReport(String nodeName) {
    NODE_NAME = nodeName;
    mStartTimeMs = System.currentTimeMillis();
    mFinishTimeMs = mStartTimeMs;
    mSuccess = false;
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
