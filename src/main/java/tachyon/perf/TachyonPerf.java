package tachyon.perf;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.client.TachyonFS;
import tachyon.perf.basic.PerfTask;
import tachyon.perf.basic.TaskConfiguration;
import tachyon.perf.basic.TaskContext;
import tachyon.perf.basic.TaskType;
import tachyon.perf.conf.PerfConf;

/**
 * Entry point for a tachyon-perf process
 */
public class TachyonPerf {
  private static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  public static void main(String[] args) {
    if (args.length < 3) {
      LOG.error("Wrong program arguments. Should be <NODENAME> <NODEID> <TaskType>"
          + "See more in bin/tachyon-perf");
      System.exit(-1);
    }

    String nodeName = null;
    int nodeId = -1;
    String taskType = null;
    try {
      nodeName = args[0];
      nodeId = Integer.parseInt(args[1]);
      taskType = args[2];
    } catch (Exception e) {
      LOG.error("Failed to parse the input args", e);
      System.exit(-1);
    }

    try {
      TachyonFS tfs = TachyonFS.get(PerfConf.get().TFS_ADDRESS);
      while (!tfs.exist(PerfConf.get().TFS_DIR + "/SYNC_START_SIGNAL")) {
        Thread.sleep(500);
      }
      tfs.close();

      TaskConfiguration taskConf = TaskConfiguration.get(taskType, true);
      PerfTask task = TaskType.get().getTaskClass(taskType);
      task.initialSet(nodeId, nodeName, taskType, taskConf);
      TaskContext taskContext = TaskType.get().getTaskContextClass(taskType);
      taskContext.initialSet(nodeId, nodeName, taskType);
      if (!task.setup(taskContext)) {
        LOG.error("Failed to setup task");
        System.exit(-1);
      }
      if (!task.run(taskContext)) {
        LOG.error("Failed to start task");
        System.exit(-1);
      }
      if (!task.cleanup(taskContext)) {
        LOG.error("Failed to cleanup the task");
        System.exit(-1);
      }
    } catch (Exception e) {
      LOG.error("Error in task", e);
    }
  }
}
