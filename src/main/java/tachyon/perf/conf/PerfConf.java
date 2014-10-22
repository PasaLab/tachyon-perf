package tachyon.perf.conf;

import java.io.File;

import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableList;

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
  public final String FS_ADDRESS;
  public final String FS_WORK_DIR;
  public final int THREADS_NUM;

  public final boolean FAILED_THEN_ABORT;
  public final int FAILED_PERCENTAGE;

  public final String TACHYON_PERF_MASTER_HOSTNAME;
  public final int TACHYON_PERF_MASTER_PORT;
  public final long UNREGISTER_TIMEOUT_MS;

  public final ImmutableList<String> GLUSTER_PREFIX = ImmutableList.of("glusterfs:///");
  public final ImmutableList<String> HDFS_PREFIX = ImmutableList.of("hdfs://");
  public final ImmutableList<String> LFS_PREFIX = ImmutableList.of("file://");
  public final ImmutableList<String> TFS_PREFIX = ImmutableList.of("tachyon://", "tachyon-ft://");

  public final String GLUSTERFS_IMPL;
  public final String GLUSTERFS_VOLUMES;
  public final String GLUSTERFS_MOUNTS;

  public final String HDFS_IMPL;

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
    FS_ADDRESS = getProperty("tachyon.perf.fs.address", "tachyon://master:19998");
    FS_WORK_DIR = getProperty("tachyon.perf.fs.work.dir", "/tmp/tachyon-perf-workspace");
    OUT_FOLDER = getProperty("tachyon.perf.out.dir", TACHYON_PERF_HOME + "/result");
    THREADS_NUM = getIntProperty("tachyon.perf.threads.num", 1);

    FAILED_THEN_ABORT = getBooleanProperty("tachyon.perf.failed.abort", true);
    FAILED_PERCENTAGE = getIntProperty("tachyon.perf.failed.percentage", 1);

    TACHYON_PERF_MASTER_HOSTNAME = getProperty("tachyon.perf.master.hostname", "master");
    TACHYON_PERF_MASTER_PORT = getIntProperty("tachyon.perf.master.port", 23333);
    UNREGISTER_TIMEOUT_MS = getLongProperty("tachyon.perf.unregister.timeout.ms", 10000);

    GLUSTERFS_IMPL =
        getProperty("tachyon.perf.glusterfs.impl",
            "org.apache.hadoop.fs.glusterfs.GlusterFileSystem");
    GLUSTERFS_VOLUMES = getProperty("tachyon.perf.glusterfs.volumes", null);
    GLUSTERFS_MOUNTS = getProperty("tachyon.perf.glusterfs.mounts", null);

    HDFS_IMPL =
        getProperty("tachyon.perf.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
  }
}
