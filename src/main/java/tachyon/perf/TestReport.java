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
// TODO: the report is too simple now, add the d3
public class TestReport {
  private static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  private final PerfConf PERF_CONF;

  private TestType mTestType;
  private int mThreadNum;

  private long[] mTestTimeMs;
  private long mTotalBytes;
  private int mSuccessFiles;

  public TestReport(PerfThread[] perfThreads, TestType testType) {
    PERF_CONF = PerfConf.get();
    mTestType = testType;
    mThreadNum = perfThreads.length;
    mTestTimeMs = new long[mThreadNum];
    for (int i = 0; i < mThreadNum; i ++) {
      ThreadReport report = perfThreads[i].getReport();
      mTestTimeMs[i] = report.getEndTimeMs() - report.getStartTimeMs();
      mTotalBytes += report.getTotalSizeByte();
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

  private String parseSizeByte(long bytes) {
    final String[] UNITS = { "B", "KB", "MB", "GB", "TB", "PB", "EB" };
    float ret = bytes;
    int index = 0;
    while ((ret >= 1024) && (index < UNITS.length - 1)) {
      ret /= 1024;
      index ++;
    }
    return ret + UNITS[index];
  }

  // TODO: generate with d3
  public void generateReport() throws IOException {
    StringBuffer sb = new StringBuffer();

    int cpus = Runtime.getRuntime().availableProcessors();
    sb.append(cpus + "\n");

    long workerSpaceBytes = tachyon.conf.WorkerConf.get().MEMORY_SIZE;
    sb.append(workerSpaceBytes + "\n");

    sb.append(mTestType + "\n");
    sb.append(mThreadNum + "\n");
    sb.append(mSuccessFiles + "\n");
    sb.append(parseSizeByte(mTotalBytes) + "\n");
    for (int i = 0; i < mThreadNum; i ++) {
      sb.append(mTestTimeMs[i] + "\n");
    }

    File outDir = new File(PERF_CONF.OUT_FOLDER);
    if (!outDir.exists()) {
      outDir.mkdirs();
    }

    File reportFile = new File(PERF_CONF.OUT_FOLDER + "/report");
    FileOutputStream fos = new FileOutputStream(reportFile);
    fos.write(sb.toString().getBytes());
    LOG.info("Report generated at path: " + PERF_CONF.OUT_FOLDER);
  }
}
