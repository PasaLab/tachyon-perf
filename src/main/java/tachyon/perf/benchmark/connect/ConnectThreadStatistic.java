package tachyon.perf.benchmark.connect;

public class ConnectThreadStatistic {
  private long mFinishTimeMs;
  private long mStartTimeMs;
  private boolean mSuccess;
  private int mSuccessOps;

  public ConnectThreadStatistic() {
    mStartTimeMs = System.currentTimeMillis();
    mFinishTimeMs = mStartTimeMs;
    mSuccess = false;
    mSuccessOps = 0;
  }

  public void addOps(int n) {
    mSuccessOps += n;
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

  public int getSuccessOps() {
    return mSuccessOps;
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
