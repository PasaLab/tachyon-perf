package tachyon.perf.benchmark.write;

/**
 * Record statistics for write thread.
 */
public class WriteThreadStatistic {
  private long mFinishTimeMs;
  private long mStartTimeMs;
  private boolean mSuccess;
  private long mSuccessBytes;
  private int mSuccessFiles;

  public WriteThreadStatistic() {
    mStartTimeMs = System.currentTimeMillis();
    mFinishTimeMs = mStartTimeMs;
    mSuccess = false;
    mSuccessBytes = 0;
    mSuccessFiles = 0;
  }

  public void addSuccessBytes(long num) {
    mSuccessBytes += num;
  }

  public void addSuccessFiles(int num) {
    mSuccessFiles += num;
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

  public long getSuccessBytes() {
    return mSuccessBytes;
  }

  public int getSuccessFiles() {
    return mSuccessFiles;
  }

  public void setFinishTimeMs(long finishTimeMs) {
    mFinishTimeMs = finishTimeMs;
  }

  public void setStartTimeMs(long startTimeMs) {
    mStartTimeMs = startTimeMs;
  }

  public void setSuccess(boolean success) {
    mSuccess = success;
  }
}
