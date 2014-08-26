package tachyon.perf.thread;

/**
 * Record statistics for read thread and write thread.
 */
public class RWThreadReport extends ThreadReport {
  private long mSuccessBytes = 0;
  private int mSuccessFiles = 0;

  public void addSuccessBytes(long num) {
    mSuccessBytes += num;
  }

  public void addSuccessFiles(int num) {
    mSuccessFiles += num;
  }

  public long getSuccessBytes() {
    return mSuccessBytes;
  }

  public int getSuccessFiles() {
    return mSuccessFiles;
  }
}
