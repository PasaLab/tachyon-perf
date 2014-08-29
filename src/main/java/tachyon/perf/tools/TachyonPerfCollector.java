package tachyon.perf.tools;

import java.io.File;
import java.io.IOException;

import tachyon.perf.basic.PerfTotalReport;
import tachyon.perf.basic.TaskType;
import tachyon.perf.conf.PerfConf;

/**
 * Generate a total report for the specified test.
 */
public class TachyonPerfCollector {
  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Wrong program arguments. Should be <TaskType> <reports dir>");
      System.exit(-1);
    }

    try {
      TaskType taskType = TaskType.getTaskType(args[0]);
      File contextsDir = new File(args[1]);
      File[] contextFiles = contextsDir.listFiles();
      if (contextFiles == null || contextFiles.length == 0) {
        throw new IOException("No task context files exists under " + args[1]);
      }
      PerfTotalReport summaryReport = PerfTotalReport.get(taskType);
      summaryReport.initialFromTaskContexts(contextFiles);
      String outputFileName = PerfConf.get().OUT_FOLDER + "/TachyonPerfReport-" + args[0];
      summaryReport.writeToFile(outputFileName);
      System.out.println("Report generated at " + outputFileName);
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("Failed to generate Tachyon-Perf-Report");
      System.exit(-1);
    }
  }
}
