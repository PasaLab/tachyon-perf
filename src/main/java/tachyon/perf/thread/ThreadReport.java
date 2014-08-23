package tachyon.perf.thread;

public abstract class ThreadReport {
  protected long mFinishTimeMs;
  protected long mStartTimeMs;
  protected boolean mSuccess = false;

  protected ThreadReport() {
    mStartTimeMs = System.currentTimeMillis();
    mFinishTimeMs = mStartTimeMs;
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
