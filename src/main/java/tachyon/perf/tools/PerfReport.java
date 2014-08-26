package tachyon.perf.tools;

import java.io.File;
import java.io.IOException;

import tachyon.perf.task.TaskType;

/**
 * The abstract class of Tachyon-Perf Report. For new test, if you want TachyonPerfCollector to
 * generate a total report for you, you should create a new class which extends this.
 */
public abstract class PerfReport {
  public static PerfReport get(TaskType taskType) throws IOException {
    if (taskType.isRead()) {
      return new ReadReport(taskType);
    } else if (taskType.isWrite()) {
      return new WriteReport(taskType);
    }
    /* Add your own Report here */
    else {
      throw new IOException("Unsupported TaskType in PerfReport");
    }
  }

  protected final TaskType TASK_TYPE;

  protected PerfReport(TaskType taskType) {
    TASK_TYPE = taskType;
  }

  /**
   * Load the statistics of all the nodes and initial this total report.
   * 
   * @param taskReportFiles
   *          the statistics files for all the nodes
   * @throws IOException
   */
  public abstract void initialFromTaskReports(File[] taskReportFiles) throws IOException;

  /**
   * Output this total report to file.
   * 
   * @param fileName
   *          the name of the output file
   * @throws IOException
   */
  public abstract void writeToFile(String fileName) throws IOException;
}
