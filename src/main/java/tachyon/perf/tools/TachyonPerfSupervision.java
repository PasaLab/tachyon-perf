package tachyon.perf.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.client.TachyonFS;
import tachyon.org.apache.thrift.TException;
import tachyon.perf.PerfConstants;
import tachyon.perf.basic.PerfTask;
import tachyon.perf.basic.Supervisible;
import tachyon.perf.basic.TaskConfiguration;
import tachyon.perf.basic.TaskType;
import tachyon.perf.conf.PerfConf;

/**
 * Monitor the slave states.
 */
public class TachyonPerfSupervision {
  private static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);
  private static final int SLAVE_STATE_FAILED = -1;
  private static final int SLAVE_STATE_INITIAL = 0;
  private static final int SLAVE_STATE_RUNNING = 1;
  private static final int SLAVE_STATE_SUCCESS = 2;

  private static int[] sSlaveStates;
  private static PerfTask[] sSlaveTasks;

  private static boolean allFinished() {
    int slavesNum = sSlaveStates.length;
    int finishedNum = 0;
    for (int i = 0; i < slavesNum; i++) {
      if (sSlaveStates[i] == SLAVE_STATE_FAILED || sSlaveStates[i] == SLAVE_STATE_SUCCESS) {
        finishedNum++;
      }
    }
    return (finishedNum == slavesNum);
  }

  private static boolean needAbort(int round, int percentage) {
    int failedSlaves = 0;
    for (int state : sSlaveStates) {
      if ((state == SLAVE_STATE_FAILED) || (state == SLAVE_STATE_INITIAL && round > 10)) {
        failedSlaves++;
      }
    }
    int failedThreshold = percentage * sSlaveStates.length / 100;
    return (failedSlaves > failedThreshold);
  }

  private static void printSlaveStatus(boolean debug, List<String> slaves) {
    int runningSlaves = 0;
    int successSlaves = 0;
    int failedSlaves = 0;
    StringBuffer sbRunningSlaves = null;
    StringBuffer sbSuccessSlaves = null;
    StringBuffer sbFailedSlaves = null;
    if (debug) {
      sbRunningSlaves = new StringBuffer("Running:");
      sbSuccessSlaves = new StringBuffer("Success:");
      sbFailedSlaves = new StringBuffer("Failed:");
    }
    for (int i = 0; i < sSlaveStates.length; i++) {
      if (sSlaveStates[i] == SLAVE_STATE_RUNNING) {
        runningSlaves++;
        if (debug) {
          sbRunningSlaves.append(" " + i + "@" + slaves.get(i));
        }
      } else if (sSlaveStates[i] == SLAVE_STATE_SUCCESS) {
        successSlaves++;
        if (debug) {
          sbSuccessSlaves.append(" " + i + "@" + slaves.get(i));
        }
      } else if (sSlaveStates[i] == SLAVE_STATE_FAILED) {
        failedSlaves++;
        if (debug) {
          sbFailedSlaves.append(" " + i + "@" + slaves.get(i));
        }
      }
    }
    String status =
        "Running: " + runningSlaves + " slaves. Success: " + successSlaves + " slaves. Failed: "
            + failedSlaves + " slaves.";
    if (debug) {
      status =
          status + "\n\t" + sbRunningSlaves.toString() + "\n\t" + sbSuccessSlaves.toString()
              + "\n\t" + sbFailedSlaves.toString();
    }
    System.out.println(status);
    LOG.info(status);
    System.out.println();
  }

  public static void main(String[] args) {
    int slavesNum = 0;
    List<String> slaves = null;
    String taskType = null;
    try {
      int index = 0;
      slavesNum = Integer.parseInt(args[0]);
      slaves = new ArrayList<String>(slavesNum);
      for (index = 1; index < slavesNum + 1; index++) {
        slaves.add(args[index]);
      }
      taskType = args[index];
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Wrong arguments. Should be <SlavesNum> [Slaves...] <TaskType>");
      LOG.error("Wrong arguments. Should be <SlavesNum> [Slaves...] <TaskType>", e);
      System.exit(-1);
    }

    try {
      TaskConfiguration taskConf = TaskConfiguration.get(taskType, true);
      sSlaveStates = new int[slavesNum];
      sSlaveTasks = new PerfTask[slavesNum];
      for (int i = 0; i < slavesNum; i++) {
        sSlaveStates[i] = SLAVE_STATE_INITIAL;
        sSlaveTasks[i] = TaskType.get().getTaskClass(taskType);
        sSlaveTasks[i].initialSet(i, slaves.get(i), taskType, taskConf);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Wrong arguments for task.");
      LOG.error("Wrong arguments for task.", e);
      System.exit(-1);
    }

    if (!(sSlaveTasks[0] instanceof Supervisible)) {
      LOG.warn("TaskType " + taskType.toString() + " doesn't support Supervisible.");
      System.out.println("TaskType " + taskType.toString() + " doesn't support Supervisible.");
      System.out.println("TachyonPerfSupervision Exit.");
      System.exit(0);
    }

    PerfConf perfConf = PerfConf.get();
    try {
      TachyonFS tfs = TachyonFS.get(perfConf.TFS_ADDRESS);
      if (!tfs.exist(perfConf.TFS_DIR + "/SYNC_START_SIGNAL")) {
        tfs.createFile(perfConf.TFS_DIR + "/SYNC_START_SIGNAL");
      }

      int round = 0;
      while (!allFinished()) {
        Thread.sleep(2000);
        for (int i = 0; i < sSlaveStates.length; i++) {
          if (sSlaveStates[i] == SLAVE_STATE_INITIAL) {
            String readyPath = ((Supervisible) sSlaveTasks[i]).getTfsReadyPath();
            if (tfs.exist(readyPath)) {
              tfs.delete(readyPath, true);
              sSlaveStates[i] = SLAVE_STATE_RUNNING;
              String runningInfo = "Slave-" + i + "(" + slaves.get(i) + ") is running";
              System.out.println(" [ " + runningInfo + " ]");
              LOG.info(runningInfo);
            }
          } else if (sSlaveStates[i] == SLAVE_STATE_RUNNING) {
            String failedPath = ((Supervisible) sSlaveTasks[i]).getTfsFailedPath();
            String successPath = ((Supervisible) sSlaveTasks[i]).getTfsSuccessPath();
            if (tfs.exist(failedPath)) {
              tfs.delete(failedPath, true);
              sSlaveStates[i] = SLAVE_STATE_FAILED;
              String failedInfo = "Failed: Slave-" + i + "(" + slaves.get(i) + ")";
              System.out.println(" [ " + failedInfo + " ]");
              LOG.info(failedInfo);
            } else if (tfs.exist(successPath)) {
              tfs.delete(successPath, true);
              sSlaveStates[i] = SLAVE_STATE_SUCCESS;
              String successInfo = "Success: Slave-" + i + "(" + slaves.get(i) + ")";
              System.out.println(" [ " + successInfo + " ]");
              LOG.info(successInfo);
            }
          }
        }
        round++;
        printSlaveStatus(perfConf.STATUS_DEBUG, slaves);
        if (perfConf.FAILED_THEN_ABORT && needAbort(round, perfConf.FAILED_PERCENTAGE)) {
          java.lang.Runtime.getRuntime().exec(
              perfConf.TACHYON_PERF_HOME + "/bin/tachyon-perf-abort");
          System.out.println("Enough slaves failed. Abort all the slaves.");
          LOG.error("Enough slaves failed. Abort all the slaves.");
          break;
        }
      }
      if (tfs.exist(perfConf.TFS_DIR + "/SYNC_START_SIGNAL")) {
        tfs.delete(perfConf.TFS_DIR + "/SYNC_START_SIGNAL", false);
      }
	  for (PerfTask task : sSlaveTasks) {
        String cleanup = ((Supervisible) task).cleanupWorkspace();
        if (cleanup != null) {
          tfs.delete(cleanup, true);
        }
      }
      tfs.close();
    } catch (TException e) {
      LOG.warn("Failed to close TachyonFS.", e);
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("Error when wait all slaves.");
      LOG.error("Error when wait all slaves.", e);
    } catch (InterruptedException e) {
      e.printStackTrace();
      System.err.println("Error when thread sleep.");
      LOG.error("Error when thread sleep.", e);
    }
    System.out.println("Tachyon-Perf End.");
  }
}
