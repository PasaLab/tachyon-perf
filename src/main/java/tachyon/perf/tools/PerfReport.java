package tachyon.perf.tools;

import java.io.File;
import java.io.IOException;

import tachyon.perf.task.TaskType;

public abstract class PerfReport {
  public static PerfReport get(TaskType taskType) throws IOException {
    if (taskType.isRead()) {
      return new ReadReport(taskType);
    } else if (taskType.isWrite()) {
      return new WriteReport(taskType);
    } else {
      throw new IOException("Unsupported TaskType in PerfReport");
    }
  }

  protected final TaskType TASK_TYPE;

  protected PerfReport(TaskType taskType) {
    TASK_TYPE = taskType;
  }

  public abstract void initialFromTaskReports(File[] taskReportFiles) throws IOException;

  public abstract void writeToFile(String fileName) throws IOException;
}
