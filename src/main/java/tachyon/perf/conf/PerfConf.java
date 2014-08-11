package tachyon.perf.conf;

import java.io.File;

import org.apache.log4j.Logger;

/**
 * Tachyon-Perf Configurations
 */
public class PerfConf extends Utils {
  private static final Logger LOG = Logger.getLogger("");

  private static PerfConf PERF_CONF = null;

  public static synchronized PerfConf get() {
    if (PERF_CONF == null) {
      PERF_CONF = new PerfConf();
    }
    return PERF_CONF;
  }

  public final String TACHYON_PERF_HOME;

  public final long FILE_LENGTH;
  public final String OUT_FOLDER;
  public final int READ_FILES_PER_THREAD;
  public final boolean READ_IDENTICAL;
  public final String READ_MODE;
  public final int READ_THREADS_NUM;
  public final String TFS_ADDRESS;
  public final String TFS_DIR;
  public final int WRITE_FILES_PER_THREAD;
  public final int WRITE_THREADS_NUM;

  private PerfConf() {
    if (System.getProperty("tachyon.perf.home") == null) {
      LOG.warn("tachyon.perf.home is not set. Using /tmp/tachyon_perf_default_home as the default value.");
      File file = new File("/tmp/tachyon_perf_default_home");
      if (!file.exists()) {
        file.mkdirs();
      }
    }
    TACHYON_PERF_HOME = getProperty("tachyon.perf.home", "/tmp/tachyon_perf_default_home");

    TFS_ADDRESS = getProperty("tachyon.perf.tfs.address", "tachyon://master:19998");
    TFS_DIR = getProperty("tachyon.perf.tfs.dir", "/tachyon-perf-workspace");
    READ_THREADS_NUM = getIntProperty("tachyon.perf.read.threads.num", 4);
    WRITE_THREADS_NUM = getIntProperty("tachyon.perf.write.threads.num", 4);
    FILE_LENGTH =
        getLongProperty("tachyon.perf.write.file.length.bytes", 128 * tachyon.Constants.MB);
    READ_FILES_PER_THREAD = getIntProperty("tachyon.perf.read.files.per.thread", 10);
    READ_IDENTICAL = getBooleanProperty("tachyon.perf.read.identical", false);
    READ_MODE = getProperty("tachyon.perf.read.mode", "RANDOM");
    WRITE_FILES_PER_THREAD = getIntProperty("tachyon.perf.write.files.per.thread", 10);
    OUT_FOLDER = getProperty("tachyon.perf.out.dir", TACHYON_PERF_HOME + "/result");
  }
}
