package tachyon.perf;

/**
 * Tachyon-Perf contants
 */
public class PerfConstants {
  public static final String PERF_LOGGER_TYPE = System.getProperty("tachyon.perf.logger.type", "");
  public static final String[] PERF_MEMORY_UNITS = { "B", "KB", "MB", "GB", "TB", "PB", "EB" };
  public static final String PERF_REPORT_FILE_NAME_PREFIX = "report";

  public static String parseSizeByte(long bytes) {
    float ret = bytes;
    int index = 0;
    while ((ret >= 1024) && (index < PERF_MEMORY_UNITS.length - 1)) {
      ret /= 1024;
      index ++;
    }
    return ret + PERF_MEMORY_UNITS[index];
  }
}
