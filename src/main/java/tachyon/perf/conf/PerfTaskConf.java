package tachyon.perf.conf;

/**
 * Tachyon-Perf configurations for task. For new test, you may add some configurations here and
 * modify the conf/tachyon-perf-env.sh
 */
public class PerfTaskConf extends Utils {
  private static PerfTaskConf PERF_TASK_CONF = null;

  public static synchronized PerfTaskConf get() {
    if (PERF_TASK_CONF == null) {
      PERF_TASK_CONF = new PerfTaskConf();
    }
    return PERF_TASK_CONF;
  }

  public final int READ_FILES_PER_THREAD;
  public final int READ_GRAIN_BYTES;
  public final boolean READ_IDENTICAL;
  public final String READ_MODE;
  public final int READ_THREADS_NUM;

  public final long WRITE_FILE_LENGTH;
  public final int WRITE_FILES_PER_THREAD;
  public final int WRITE_GRAIN_BYTES;
  public final int WRITE_THREADS_NUM;

  private PerfTaskConf() {
    READ_FILES_PER_THREAD = getIntProperty("tachyon.perf.read.files.per.thread", 10);
    READ_IDENTICAL = getBooleanProperty("tachyon.perf.read.identical", false);
    READ_MODE = getProperty("tachyon.perf.read.mode", "RANDOM");
    READ_THREADS_NUM = getIntProperty("tachyon.perf.read.threads.num", 4);
    READ_GRAIN_BYTES = getIntProperty("tachyon.perf.read.grain.bytes", 4 * tachyon.Constants.MB);

    WRITE_FILE_LENGTH =
        getLongProperty("tachyon.perf.write.file.length.bytes", 128 * tachyon.Constants.MB);
    WRITE_FILES_PER_THREAD = getIntProperty("tachyon.perf.write.files.per.thread", 10);
    WRITE_GRAIN_BYTES = getIntProperty("tachyon.perf.write.grain.bytes", 4 * tachyon.Constants.MB);
    WRITE_THREADS_NUM = getIntProperty("tachyon.perf.write.threads.num", 4);
  }
}
