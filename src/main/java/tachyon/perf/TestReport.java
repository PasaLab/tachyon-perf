package tachyon.perf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import tachyon.perf.conf.PerfConf;
import tachyon.perf.thread.PerfThread;
import tachyon.perf.thread.ThreadReport;

/**
 * The test report for a Tachyon-Perf process
 */
public class TestReport {
  private static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  private final PerfConf PERF_CONF;
  private final long START_TIME;

  private TestType mTestType;
  private String mRWType;
  private int mThreadNum;

  private long[] mTestTimeMs;
  private long[] mTestBytes;
  private boolean mSuccess;
  private int mSuccessFiles;

  public TestReport(long startTimeMs, PerfThread[] perfThreads, TestType testType, String rwType) {
    PERF_CONF = PerfConf.get();
    START_TIME = startTimeMs;
    mTestType = testType;
    mRWType = rwType;
    mThreadNum = perfThreads.length;
    mTestTimeMs = new long[mThreadNum];
    mTestBytes = new long[mThreadNum];
    mSuccess = true;
    for (int i = 0; i < mThreadNum; i ++) {
      ThreadReport report = perfThreads[i].getReport();
      mTestTimeMs[i] = report.getEndTimeMs() - report.getStartTimeMs();
      mTestBytes[i] = report.getTotalSizeByte();
      if (!report.getSuccess()) {
        mSuccess = false;
      }
      mSuccessFiles += report.getSuccessFileNum();
    }
  }

  private String getAverageTime() {
    long averageTimeMs = 0;
    for (int i = 0; i < mThreadNum; i ++) {
      averageTimeMs += mTestTimeMs[i];
    }
    averageTimeMs /= mThreadNum;

    long averageTimeS = averageTimeMs / 1000;
    long days = averageTimeS / (24 * 60 * 60);
    long hours = averageTimeS % (24 * 60 * 60) / (60 * 60);
    long minutes = averageTimeS % (60 * 60) / 60;
    long seconds = averageTimeS % 60;

    return days + "d-" + hours + "h-" + minutes + "m-" + seconds + "s";
  }

  public void generateReport() throws IOException {
    StringBuffer sb = new StringBuffer();

    sb.append(mSuccess + "\n");

    int cpus = Runtime.getRuntime().availableProcessors();
    sb.append(cpus + "\n");

    long workerSpaceBytes = tachyon.conf.WorkerConf.get().MEMORY_SIZE;
    sb.append(workerSpaceBytes + "\n");

    sb.append(mRWType + "\n");
    sb.append(START_TIME + "\n");
    // sb.append(mTestType + "\n");
    sb.append(mThreadNum + "\n");
    // sb.append(mSuccessFiles + "\n");

    for (int i = 0; i < mThreadNum; i ++) {
      sb.append(mTestBytes[i] + "\n");
      sb.append(mTestTimeMs[i] + "\n");
    }

    File outDir = new File(PERF_CONF.OUT_FOLDER);
    if (!outDir.exists()) {
      outDir.mkdirs();
    }

    File reportFile = new File(PERF_CONF.OUT_FOLDER + "/report_" + mTestType);
    FileOutputStream fos = new FileOutputStream(reportFile);
    fos.write(sb.toString().getBytes());
    LOG.info("Report generated at path: " + PERF_CONF.OUT_FOLDER);
  }

  public boolean getSuccess() {
    return mSuccess;
  }
}
