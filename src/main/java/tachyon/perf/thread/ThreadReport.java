package tachyon.perf.thread;

/**
 * The report for each test thread
 */
public class ThreadReport {
  private long mStartTimeMs;
  private long mEndTimeMs;

  private int mSuccessFileNum;
  private long mTotalSizeByte;

  public ThreadReport() {
    mSuccessFileNum = 0;
    mTotalSizeByte = 0;
  }

  public void end() {
    mEndTimeMs = System.currentTimeMillis();
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
