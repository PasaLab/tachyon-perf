package tachyon.perf.thread;

/**
 * The report for each test thread
 */
public class ThreadReport {
  private long mStartTimeMs;
  private long mEndTimeMs;

  private int mSuccessFileNum;
  private long mTotalSizeByte;

  private boolean mSuccess;

  public ThreadReport() {
    mSuccessFileNum = 0;
    mTotalSizeByte = 0;
    mSuccess = true;
  }

  public void end() {
    mEndTimeMs = System.currentTimeMillis();
  }

  public void error() {
    mSuccess = false;
  }

  public boolean getSuccess() {
    return mSuccess;
  }

  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  public long getEndTimeMs() {
    return mEndTimeMs;
  }

  public int getSuccessFileNum() {
    return mSuccessFileNum;
  }

  public long getTotalSizeByte() {
    return mTotalSizeByte;
  }

  public void start() {
    mStartTimeMs = System.currentTimeMillis();
  }

  public void successFiles(int successFileNum) {
    mSuccessFileNum += successFileNum;
  }

  public void successContent(long successSizeByte) {
    mTotalSizeByte += successSizeByte;
  }
}
