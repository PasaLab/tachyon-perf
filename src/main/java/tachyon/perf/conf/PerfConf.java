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

  public final String OUT_FOLDER;
  public final boolean STATUS_DEBUG;
  public final String TFS_ADDRESS;
  public final String TFS_DIR;

  public final boolean FAILED_THEN_ABORT;
  public final int FAILED_PERCENTAGE;

  private PerfConf() {
    if (System.getProperty("tachyon.perf.home") == null) {
      LOG.warn("tachyon.perf.home is not set. Using /tmp/tachyon_perf_default_home as the default value.");
      File file = new File("/tmp/tachyon_perf_default_home");
      if (!file.exists()) {
        file.mkdirs();
      }
    }
    TACHYON_PERF_HOME = getProperty("tachyon.perf.home", "/tmp/tachyon_perf_default_home");
    STATUS_DEBUG = getBooleanProperty("tachyon.perf.status.debug", false);
    TFS_ADDRESS = getProperty("tachyon.perf.tfs.address", "tachyon://localhost:19998");
    TFS_DIR = getProperty("tachyon.perf.tfs.dir", "/tachyon-perf-workspace");
    OUT_FOLDER = getProperty("tachyon.perf.out.dir", TACHYON_PERF_HOME + "/result");

    FAILED_THEN_ABORT = getBooleanProperty("tachyon.perf.failed.then.abort", true);
    FAILED_PERCENTAGE = getIntProperty("tachyon.perf.failed.percentage", 1);
  }
}
