package tachyon.perf.basic;

import java.io.File;
import java.io.IOException;

import tachyon.perf.basic.TaskType;
import tachyon.perf.benchmark.read.ReadTotalReport;
import tachyon.perf.benchmark.write.WriteTotalReport;

/**
 * The abstract class of Tachyon-Perf Total Report. For new test, if you want TachyonPerfCollector
 * to generate a total report for you, you should create a new class which extends this.
 */
public abstract class PerfTotalReport {
  public static PerfTotalReport get(TaskType taskType) throws IOException {
    if (taskType.isRead()) {
      return new ReadTotalReport(taskType);
    } else if (taskType.isWrite()) {
      return new WriteTotalReport(taskType);
    }
    /* Add your own Report here */
    else {
      throw new IOException("Unsupported TaskType in PerfReport");
    }
  }

  protected final TaskType TASK_TYPE;

  protected PerfTotalReport(TaskType taskType) {
    TASK_TYPE = taskType;
  }

  /**
   * Load the statistics of all the nodes and initial this total report.
   * 
   * @param taskContextFiles
   *          the statistics files for all the nodes
   * @throws IOException
   */
  public abstract void initialFromTaskContexts(File[] taskContextFiles) throws IOException;

  /**
   * Output this total report to file.
   * 
   * @param fileName
   *          the name of the output file
   * @throws IOException
   */
  public abstract void writeToFile(String fileName) throws IOException;
}
