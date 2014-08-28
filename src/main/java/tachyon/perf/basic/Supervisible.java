package tachyon.perf.basic;

/**
 * Interface for Supervision supporting. For new test, if you want TachyonPerfSupervision to monitor
 * the node states, you should implement this interface in your own PerfTask. Make sure that for
 * each method it returns the same value since the test is created.
 */
public interface Supervisible {
  /**
   * If the test failed, it will write a failed file to TachyonFS, and this is the file path.
   * 
   * @return the path of the failed file
   */
  public String getTfsFailedPath();

  /**
   * If the test is setup, it will write a ready file to TachyonFS, and this is the file path.
   * 
   * @return the path of the ready file
   */
  public String getTfsReadyPath();

  /**
   * If the test succeeded, it will write a success file to TachyonFS, and this is the file path.
   * 
   * @return the path of the success file
   */
  public String getTfsSuccessPath();
}
