package tachyon.perf.thread;

import org.apache.log4j.Logger;

import tachyon.perf.PerfConstants;

/**
 * The base test thread class for Tachyon-Perf. Now it's used for read test and write test.
 */
public abstract class PerfThread implements Runnable {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  public final int ID;

  protected ThreadReport mThreadReport;

  protected PerfThread(int id) {
    ID = id;
  }

  public ThreadReport getReport() {
    return mThreadReport;
  }
}
