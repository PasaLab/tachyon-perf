package tachyon.perf.thread;

import org.apache.log4j.Logger;

import tachyon.perf.PerfConstants;

/**
 * The base test thread class for Tachyon-Perf
 */
public abstract class PerfThread implements Runnable {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  public final int ID;

  protected String mTfsAddress;
  protected ThreadReport mThreadReport;

  protected PerfThread(int id, String tfsAddress) {
    ID = id;
    mTfsAddress = tfsAddress;
    mThreadReport = new ThreadReport();
  }

  public ThreadReport getReport() {
    return mThreadReport;
  }
}
