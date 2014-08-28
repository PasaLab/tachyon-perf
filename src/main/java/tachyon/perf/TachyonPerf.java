package tachyon.perf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.perf.basic.PerfTask;
import tachyon.perf.basic.TaskReport;
import tachyon.perf.basic.TaskType;

/**
 * Entry point for a tachyon-perf process
 */
public class TachyonPerf {
  private static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  public static void main(String[] args) {
    if (args.length < 3) {
      LOG.error("Wrong program arguments. Should be <NODENAME> <NODEID> <TaskType> [args...] "
          + "See more in bin/tachyon-perf");
      System.exit(-1);
    }

    int nodeId = -1;
    TaskType taskType = null;
    List<String> remainArgs = null;
    try {
      nodeId = Integer.parseInt(args[1]);
      taskType = TaskType.getTaskType(args[2]);
      remainArgs = new ArrayList<String>(args.length - 3);
      for (int i = 3; i < args.length; i ++) {
        remainArgs.add(args[i]);
      }
    } catch (Exception e) {
      LOG.error("Failed to parse the input args", e);
      System.exit(-1);
    }

    try {
      PerfTask task = PerfTask.getPerfTask(args[0], nodeId, taskType, remainArgs);
      TaskReport taskReport = TaskReport.getTaskReport(args[0], nodeId, taskType, remainArgs);
      if (!task.setup(taskReport)) {
        LOG.error("Failed to setup task");
        System.exit(-1);
      }
      if (!task.run(taskReport)) {
        LOG.error("Failed to start task");
        System.exit(-1);
      }
      if (!task.cleanup(taskReport)) {
        LOG.error("Failed to cleanup the task");
        System.exit(-1);
      }
    } catch (IOException e) {
      LOG.error("Error in task", e);
    }
  }
}
